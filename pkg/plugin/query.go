package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/sdkerr"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/ces/v1/model"
)

// Server-side query (QueryData) support. Grafana alerting, recording rules and
// any other backend-evaluated request call QueryData directly; the dashboard
// frontend keeps using the /metric-data resource endpoint.
//
// CES limits (BatchListMetricData, "Querying Monitoring Data in Batches",
// https://support.huaweicloud.com/intl/en-us/api-ces/ces_03_0034.html; see
// docs/ALERTING.md):
//   - 1..500 metrics per request, 1..4 dimensions per metric
//   - metrics * (to-from) / period <= 3000 datapoints per request
//   - maximum (to-from) span per period; CES silently moves "from" otherwise
const (
	maxMetricsPerRequest    = 500
	maxDatapointsPerRequest = 3000
	maxDimensionsPerMetric  = 4
	// Raw ("1") data is normally reported once per minute. It is used as the
	// interval when estimating the datapoint budget for period=1 queries.
	rawDataIntervalSeconds = 60

	defaultQueryTimeout   = 30 * time.Second
	maxConcurrentRequests = 4
	defaultFilter         = "average"
	defaultPeriod         = "1"
	// defaultRegion matches the query editor's displayed default and the
	// dashboard path (src/datasource.ts: target.region || 'cn-east-3').
	defaultRegion = "cn-east-3"
)

var allowedFilters = map[string]bool{
	"average": true, "max": true, "min": true, "sum": true, "variance": true,
}

// Allowed periods (seconds) and the maximum to-from span CES accepts for each
// (CES API "from" parameter). The periods are those supported by the pinned
// SDK model (v0.1.92 BatchListMetricDataRequestBody.Period).
var periodMaxSpan = map[string]time.Duration{
	"1":     4 * time.Hour,
	"300":   24 * time.Hour,
	"1200":  3 * 24 * time.Hour,
	"3600":  10 * 24 * time.Hour,
	"14400": 30 * 24 * time.Hour,
	"86400": 180 * 24 * time.Hour,
}

// Matches $var, ${var}, ${var:fmt} and [[var]] dashboard variable syntax.
var templateVariablePattern = regexp.MustCompile(`\$\{[^}]*\}|\$[A-Za-z_][A-Za-z0-9_]*|\[\[[^\]]*\]\]`)

// metricDataClient is the subset of the CES SDK client used by QueryData.
// *ces.CesClient satisfies it; tests use a fake.
type metricDataClient interface {
	BatchListMetricData(*model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error)
}

// metricClientFactory creates a CES client for the given (per-region) settings.
type metricClientFactory func(cfg *CloudEyeSettings, timeout time.Duration) (metricDataClient, error)

func defaultMetricClientFactory(cfg *CloudEyeSettings, timeout time.Duration) (metricDataClient, error) {
	return newSafeCESClient(cfg, timeout)
}

// flexString accepts a JSON string, number or null. Saved queries may store
// period as a number.
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" {
		*f = ""
		return nil
	}
	if strings.HasPrefix(s, `"`) {
		var v string
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*f = flexString(v)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("expected string or number, got %s", s)
	}
	*f = flexString(n.String())
	return nil
}

// queryModel is the JSON model saved by the query editor (src/types.ts MyQuery).
type queryModel struct {
	Region     flexString `json:"region"`
	Namespace  flexString `json:"namespace"`
	Dimstr     flexString `json:"dimstr"`
	MetricName flexString `json:"metricName"`
	Filter     flexString `json:"filter"`
	Period     flexString `json:"period"`
}

// parsedQuery is a validated query ready to send to CES.
type parsedQuery struct {
	RefID      string
	Region     string
	Namespace  string
	MetricName string
	Dimensions []model.MetricsDimension // sorted by name
	Filter     string
	Period     string
	From       int64 // unix ms
	To         int64 // unix ms
}

func (q *parsedQuery) metricKey() string {
	return metricKey(q.Namespace, q.MetricName, q.Dimensions)
}

func (q *parsedQuery) labels() data.Labels {
	l := data.Labels{"namespace": q.Namespace}
	for _, d := range q.Dimensions {
		l[d.Name] = d.Value
	}
	return l
}

// metricKey identifies a metric independent of dimension order.
func metricKey(namespace, metricName string, dims []model.MetricsDimension) string {
	parts := make([]string, 0, len(dims))
	for _, d := range dims {
		parts = append(parts, d.Name+"="+d.Value)
	}
	sort.Strings(parts)
	return namespace + "\x00" + metricName + "\x00" + strings.Join(parts, "\x00")
}

type queryError struct {
	status backend.Status
	msg    string
}

func (e *queryError) Error() string { return e.msg }

func validationError(format string, args ...interface{}) *queryError {
	return &queryError{status: backend.StatusBadRequest, msg: fmt.Sprintf(format, args...)}
}

// parseDimstr parses "name:value[,name:value...]". Values may contain ':'.
func parseDimstr(dimstr string) ([]model.MetricsDimension, error) {
	seen := map[string]bool{}
	var dims []model.MetricsDimension
	for _, item := range strings.Split(dimstr, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		kv := strings.SplitN(item, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("dimension %q must use the form name:value", item)
		}
		name, value := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if name == "" || value == "" {
			return nil, fmt.Errorf("dimension %q must have a non-empty name and value", item)
		}
		if seen[name] {
			return nil, fmt.Errorf("dimension %q is specified more than once", name)
		}
		seen[name] = true
		dims = append(dims, model.MetricsDimension{Name: name, Value: value})
	}
	if len(dims) == 0 {
		return nil, errors.New("dimstr must contain at least one name:value dimension")
	}
	if len(dims) > maxDimensionsPerMetric {
		return nil, fmt.Errorf("dimstr has %d dimensions; CES supports at most %d", len(dims), maxDimensionsPerMetric)
	}
	sort.Slice(dims, func(i, j int) bool { return dims[i].Name < dims[j].Name })
	return dims, nil
}

// estimatedDatapoints estimates the number of datapoints CES returns for one
// metric over the range, used for the per-request datapoint budget.
func estimatedDatapoints(period string, from, to int64) int64 {
	p, _ := strconv.ParseInt(period, 10, 64)
	if p < rawDataIntervalSeconds {
		p = rawDataIntervalSeconds
	}
	interval := p * 1000
	return (to-from+interval-1)/interval + 1
}

// parseQuery validates a single backend.DataQuery.
func parseQuery(q backend.DataQuery, cfg *CloudEyeSettings) (*parsedQuery, *queryError) {
	var m queryModel
	if len(q.JSON) == 0 {
		return nil, validationError("query model is empty")
	}
	if err := json.Unmarshal(q.JSON, &m); err != nil {
		return nil, validationError("invalid query model: %v", err)
	}

	fields := []struct {
		name     string
		value    string
		required bool
	}{
		{"region", strings.TrimSpace(string(m.Region)), false},
		{"namespace", strings.TrimSpace(string(m.Namespace)), true},
		{"dimstr", strings.TrimSpace(string(m.Dimstr)), true},
		{"metricName", strings.TrimSpace(string(m.MetricName)), true},
		{"filter", strings.TrimSpace(string(m.Filter)), false},
		{"period", strings.TrimSpace(string(m.Period)), false},
	}
	var missing []string
	for _, f := range fields {
		if templateVariablePattern.MatchString(f.value) {
			return nil, validationError(
				"%s %q contains a dashboard template variable; variables are not resolved for alerting and server-side queries, use an explicit value",
				f.name, f.value)
		}
		if f.required && f.value == "" {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		return nil, validationError("missing required query field(s): %s", strings.Join(missing, ", "))
	}

	pq := &parsedQuery{
		RefID:      q.RefID,
		Region:     fields[0].value,
		Namespace:  fields[1].value,
		MetricName: fields[3].value,
		Filter:     fields[4].value,
		Period:     fields[5].value,
	}
	if pq.Region == "" {
		// The query editor displays a default region without saving it, and
		// the dashboard path sends the same default. In Specific Region Mode
		// use the configured region so the default never mismatches it.
		pq.Region = defaultRegion
		if isSingleRegionMode(cfg) && cfg.Region != "" {
			pq.Region = cfg.Region
		}
	}
	if pq.Region == "-" {
		// Bundled dashboard templates store "-" and set the real region from a
		// dashboard variable in the frontend; alerting never sees that value.
		return nil, validationError(
			`region "-" is a dashboard template placeholder; alert queries need an explicit region`)
	}
	if pq.Filter == "" {
		pq.Filter = defaultFilter
	}
	if pq.Period == "" {
		pq.Period = defaultPeriod
	}
	if !allowedFilters[pq.Filter] {
		return nil, validationError("unsupported filter %q; expected one of average, max, min, sum, variance", pq.Filter)
	}
	maxSpan, ok := periodMaxSpan[pq.Period]
	if !ok {
		return nil, validationError("unsupported period %q; expected one of 1, 300, 1200, 3600, 14400, 86400", pq.Period)
	}

	dims, err := parseDimstr(fields[2].value)
	if err != nil {
		return nil, validationError("invalid dimstr: %v", err)
	}
	pq.Dimensions = dims

	if isSingleRegionMode(cfg) && cfg.Region != "" && cfg.Region != pq.Region {
		return nil, validationError(
			"query region %q does not match the datasource region %q (Specific Region Mode)", pq.Region, cfg.Region)
	}

	pq.From = q.TimeRange.From.UnixMilli()
	pq.To = q.TimeRange.To.UnixMilli()
	if q.TimeRange.From.IsZero() || q.TimeRange.To.IsZero() || pq.From >= pq.To {
		return nil, validationError("invalid time range: from must be before to")
	}
	// Every allowed period's span cap keeps a single metric within the
	// 3000-datapoint budget, so only planBatches needs the budget check.
	if span := q.TimeRange.To.Sub(q.TimeRange.From); span > maxSpan {
		return nil, validationError(
			"time range %s exceeds the CES maximum of %s for period %s", span, maxSpan, pq.Period)
	}
	return pq, nil
}

// batch is one CES BatchListMetricData request. Every query in a batch shares
// region, filter, period and time range; identical metrics are requested once
// and fanned out to each RefID.
type batch struct {
	region, filter, period string
	from, to               int64
	metrics                []*parsedQuery      // unique metrics, request order
	refIDs                 map[string][]string // metricKey -> refIDs
}

func groupKey(q *parsedQuery) string {
	return fmt.Sprintf("%s|%s|%s|%d|%d", q.Region, q.Filter, q.Period, q.From, q.To)
}

// planBatches groups queries into CES requests that respect the per-request
// metric and datapoint limits. Output order is deterministic.
func planBatches(queries []*parsedQuery) []*batch {
	groups := map[string][]*parsedQuery{}
	var order []string
	for _, q := range queries {
		k := groupKey(q)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], q)
	}

	var batches []*batch
	for _, k := range order {
		qs := groups[k]
		first := qs[0]
		perMetric := estimatedDatapoints(first.Period, first.From, first.To)
		limit := int(int64(maxDatapointsPerRequest) / perMetric)
		if limit < 1 {
			limit = 1
		}
		if limit > maxMetricsPerRequest {
			limit = maxMetricsPerRequest
		}

		var cur *batch
		for _, q := range qs {
			key := q.metricKey()
			if cur != nil {
				if _, dup := cur.refIDs[key]; dup {
					cur.refIDs[key] = append(cur.refIDs[key], q.RefID)
					continue
				}
			}
			if cur == nil || len(cur.metrics) >= limit {
				cur = &batch{
					region: q.Region, filter: q.Filter, period: q.Period,
					from: q.From, to: q.To,
					refIDs: map[string][]string{},
				}
				batches = append(batches, cur)
			}
			cur.metrics = append(cur.metrics, q)
			cur.refIDs[key] = []string{q.RefID}
		}
	}
	return batches
}

func (b *batch) request() *model.BatchListMetricDataRequest {
	metrics := make([]model.MetricInfo, 0, len(b.metrics))
	for _, q := range b.metrics {
		dims := make([]model.MetricsDimension, len(q.Dimensions))
		copy(dims, q.Dimensions)
		metrics = append(metrics, model.MetricInfo{
			Namespace:  q.Namespace,
			MetricName: q.MetricName,
			Dimensions: dims,
		})
	}
	return &model.BatchListMetricDataRequest{
		Body: &model.BatchListMetricDataRequestBody{
			Metrics: metrics,
			Period:  b.period,
			Filter:  b.filter,
			From:    b.from,
			To:      b.to,
		},
	}
}

// datapointValue returns the value for the requested filter, or nil when CES
// returned no value. With period=1 (raw data) CES ignores the filter, so fall
// back to the average field, which carries the raw value.
func datapointValue(dp model.DatapointForBatchMetric, filter, period string) *float64 {
	var v *float64
	switch filter {
	case "average":
		v = dp.Average
	case "max":
		v = dp.Max
	case "min":
		v = dp.Min
	case "sum":
		v = dp.Sum
	case "variance":
		v = dp.Variance
	}
	if v == nil && period == "1" {
		v = dp.Average
	}
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

// buildFrames creates the time series frame for a query. Datapoints are sorted
// by timestamp; duplicate timestamps keep the last value; invalid timestamps
// are dropped.
//
// No data (no datapoints, or only null values) returns no frames. A zero-row
// frame with labelled fields is NOT treated as No Data by Grafana alerting: it
// reduces to a labelled NaN series and the rule stays Normal.
func buildFrames(q *parsedQuery, refID string, datapoints []model.DatapointForBatchMetric) data.Frames {
	type point struct {
		ts int64
		v  *float64
	}
	byTS := make(map[int64]*float64, len(datapoints))
	for _, dp := range datapoints {
		if dp.Timestamp <= 0 {
			continue
		}
		byTS[dp.Timestamp] = datapointValue(dp, q.Filter, q.Period)
	}
	points := make([]point, 0, len(byTS))
	for ts, v := range byTS {
		points = append(points, point{ts, v})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].ts < points[j].ts })

	hasValue := false
	for _, p := range points {
		if p.v != nil {
			hasValue = true
			break
		}
	}
	if !hasValue {
		return nil
	}

	times := make([]time.Time, len(points))
	values := make([]*float64, len(points))
	for i, p := range points {
		times[i] = time.UnixMilli(p.ts).UTC()
		values[i] = p.v
	}

	frame := data.NewFrame(q.MetricName,
		data.NewField("Time", nil, times),
		data.NewField(q.MetricName, q.labels(), values),
	)
	frame.RefID = refID
	frame.Meta = &data.FrameMeta{
		Type:        data.FrameTypeTimeSeriesMulti,
		TypeVersion: data.FrameTypeVersion{0, 1},
	}
	return data.Frames{frame}
}

// sdkErrorResponse converts an SDK/transport error into a DataResponse
// without leaking credentials (SDK errors never contain AK/SK).
func sdkErrorResponse(err error) backend.DataResponse {
	var qe *queryError
	if errors.As(err, &qe) {
		return backend.ErrDataResponse(qe.status, qe.msg)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return backend.ErrDataResponse(backend.StatusTimeout, "CES request timed out")
	}
	if errors.Is(err, context.Canceled) {
		return backend.ErrDataResponse(backend.StatusTimeout, "CES request cancelled")
	}
	// HTTP client timeouts surface as *url.Error (a net.Error).
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return backend.ErrDataResponse(backend.StatusTimeout, "CES request timed out: "+err.Error())
	}

	status := 0
	var sre *sdkerr.ServiceResponseError
	var srv sdkerr.ServiceResponseError
	switch {
	case errors.As(err, &sre):
		status = sre.StatusCode
	case errors.As(err, &srv):
		status = srv.StatusCode
	}
	var s backend.Status
	switch status {
	case 400:
		s = backend.StatusBadRequest
	case 401:
		s = backend.StatusUnauthorized
	case 403:
		s = backend.StatusForbidden
	case 404:
		s = backend.StatusNotFound
	case 408:
		s = backend.StatusTimeout
	case 429:
		s = backend.StatusTooManyRequests
	default:
		s = backend.StatusBadGateway
	}
	return backend.ErrDataResponse(s, "CES query failed: "+err.Error())
}

// runBatch executes one CES request and maps the response back to RefIDs by
// (namespace, metric name, dimensions); response order is never assumed.
func runBatch(ctx context.Context, b *batch, client metricDataClient) (map[string]backend.DataResponse, error) {
	type result struct {
		resp *model.BatchListMetricDataResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("CES client panic: %v", r)}
			}
		}()
		resp, err := client.BatchListMetricData(b.request())
		ch <- result{resp: resp, err: err}
	}()

	var res result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res = <-ch:
	}
	if res.err != nil {
		return nil, res.err
	}
	if res.resp == nil {
		return nil, &queryError{status: backend.StatusBadGateway, msg: "CES returned an empty response"}
	}

	byKey := map[string]*parsedQuery{}
	byNameDims := map[string][]*parsedQuery{} // for responses missing namespace
	for _, q := range b.metrics {
		byKey[q.metricKey()] = q
		nk := metricKey("", q.MetricName, q.Dimensions)
		byNameDims[nk] = append(byNameDims[nk], q)
	}

	points := map[string][]model.DatapointForBatchMetric{}
	if res.resp.Metrics != nil {
		for _, m := range *res.resp.Metrics {
			if m.Dimensions == nil {
				log.DefaultLogger.Warn("Ignoring CES metric without dimensions", "metric", m.MetricName)
				continue
			}
			var q *parsedQuery
			if m.Namespace != nil {
				q = byKey[metricKey(*m.Namespace, m.MetricName, *m.Dimensions)]
			} else if c := byNameDims[metricKey("", m.MetricName, *m.Dimensions)]; len(c) == 1 {
				q = c[0]
			}
			if q == nil {
				log.DefaultLogger.Warn("Ignoring unrequested CES metric in response", "metric", m.MetricName)
				continue
			}
			k := q.metricKey()
			points[k] = append(points[k], m.Datapoints...)
		}
	}

	out := map[string]backend.DataResponse{}
	for _, q := range b.metrics {
		k := q.metricKey()
		for _, refID := range b.refIDs[k] {
			out[refID] = backend.DataResponse{Status: backend.StatusOK, Frames: buildFrames(q, refID, points[k])}
		}
	}
	return out, nil
}

// queryData implements the QueryData handler.
func (ds *CloudEyeDatasource) queryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()
	if req == nil || len(req.Queries) == 0 {
		return response, nil
	}

	failAll := func(dr backend.DataResponse) (*backend.QueryDataResponse, error) {
		for _, q := range req.Queries {
			response.Responses[q.RefID] = dr
		}
		return response, nil
	}

	if req.PluginContext.DataSourceInstanceSettings == nil {
		return failAll(backend.ErrDataResponse(backend.StatusBadRequest, "datasource settings are missing"))
	}
	cfg, err := LoadSettings(req.PluginContext)
	if err != nil {
		return failAll(backend.ErrDataResponse(backend.StatusBadRequest, "invalid datasource settings: "+err.Error()))
	}
	if cfg.AK == "" || cfg.SK == "" {
		return failAll(backend.ErrDataResponse(backend.StatusUnauthorized, "access key and secret key must be configured"))
	}

	var valid []*parsedQuery
	for _, q := range req.Queries {
		pq, qerr := parseQuery(q, cfg)
		if qerr != nil {
			response.Responses[q.RefID] = backend.ErrDataResponse(qerr.status, qerr.msg)
			continue
		}
		valid = append(valid, pq)
	}
	if len(valid) == 0 {
		return response, nil
	}

	timeout := ds.queryTimeout
	if timeout <= 0 {
		timeout = defaultQueryTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	factory := ds.newMetricClient
	if factory == nil {
		factory = defaultMetricClientFactory
	}

	// One client per region; client creation may perform network calls (IAM
	// project lookup in Huawei Cloud mode), so it runs under the same timeout.
	type regionClient struct {
		once   sync.Once
		client metricDataClient
		err    error
	}
	var clientsMu sync.Mutex
	clients := map[string]*regionClient{}
	getClient := func(region string) (metricDataClient, error) {
		clientsMu.Lock()
		rc, ok := clients[region]
		if !ok {
			rc = &regionClient{}
			clients[region] = rc
		}
		clientsMu.Unlock()
		rc.once.Do(func() {
			regionCfg := *cfg
			regionCfg.Region = region
			rc.client, rc.err = buildClient(ctx, factory, &regionCfg, timeout)
		})
		return rc.client, rc.err
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentRequests)
	for _, b := range planBatches(valid) {
		b := b
		wg.Add(1)
		go func() {
			defer wg.Done()
			var results map[string]backend.DataResponse
			var err error
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
				var client metricDataClient
				client, err = getClient(b.region)
				if err == nil {
					results, err = runBatch(ctx, b, client)
				}
			case <-ctx.Done():
				err = ctx.Err()
			}

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				log.DefaultLogger.Error("CES batch query failed", "region", b.region, "metrics", len(b.metrics), "err", err.Error())
				dr := sdkErrorResponse(err)
				for _, refs := range b.refIDs {
					for _, refID := range refs {
						response.Responses[refID] = dr
					}
				}
				return
			}
			for refID, dr := range results {
				response.Responses[refID] = dr
			}
		}()
	}
	wg.Wait()
	return response, nil
}

// buildClient runs the factory under ctx and converts panics into errors. The
// SDK has no context support, so its HTTP timeout is capped at the time left
// before the ctx deadline to stop in-flight requests outliving the query.
// Factory errors keep their status when they are *queryError (for example an
// unsupported region is 400); anything else, such as an IAM project lookup
// failure, maps to 502 in sdkErrorResponse.
func buildClient(ctx context.Context, factory metricClientFactory, cfg *CloudEyeSettings, timeout time.Duration) (metricDataClient, error) {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	type result struct {
		c   metricDataClient
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("failed to create CES client: %v", r)}
			}
		}()
		c, err := factory(cfg, timeout)
		ch <- result{c, err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		if r.c == nil {
			return nil, &queryError{status: backend.StatusInternal, msg: "failed to create CES client"}
		}
		return r.c, nil
	}
}
