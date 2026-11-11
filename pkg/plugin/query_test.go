package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/sdkerr"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/ces/v1/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Dummy credentials; never real.
const (
	testAK = "TESTACCESSKEY"
	testSK = "TESTSECRETKEY"
)

var (
	testTo   = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	testFrom = testTo.Add(-10 * time.Minute)
)

type fakeCES struct {
	mu       sync.Mutex
	requests []*model.BatchListMetricDataRequest
	regions  []string
	respond  func(region string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error)
}

type fakeRegionClient struct {
	f      *fakeCES
	region string
}

func (c *fakeRegionClient) BatchListMetricData(req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
	c.f.mu.Lock()
	c.f.requests = append(c.f.requests, req)
	c.f.mu.Unlock()
	return c.f.respond(c.region, req)
}

func (f *fakeCES) factory(cfg *CloudEyeSettings, _ time.Duration) (metricDataClient, error) {
	f.mu.Lock()
	f.regions = append(f.regions, cfg.Region)
	f.mu.Unlock()
	return &fakeRegionClient{f: f, region: cfg.Region}, nil
}

// echoResponse returns one datapoint series per requested metric, in reverse
// request order, with dimensions reversed, to prove no ordering assumptions.
func echoResponse(_ string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
	var out []model.BatchMetricData
	for i := len(req.Body.Metrics) - 1; i >= 0; i-- {
		m := req.Body.Metrics[i]
		ns := m.Namespace
		dims := make([]model.MetricsDimension, 0, len(m.Dimensions))
		for j := len(m.Dimensions) - 1; j >= 0; j-- {
			dims = append(dims, m.Dimensions[j])
		}
		out = append(out, model.BatchMetricData{
			Namespace:  &ns,
			MetricName: m.MetricName,
			Dimensions: &dims,
			Datapoints: []model.DatapointForBatchMetric{
				{Timestamp: req.Body.From + 120000, Average: f64(2), Max: f64(20)},
				{Timestamp: req.Body.From + 60000, Average: f64(1), Max: f64(10)},
			},
		})
	}
	return &model.BatchListMetricDataResponse{Metrics: &out}, nil
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "i/o timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

func f64(v float64) *float64 { return &v }
func strp(s string) *string  { return &s }

func pluginCtx(jsonData string, secure map[string]string) backend.PluginContext {
	return backend.PluginContext{DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
		JSONData:                []byte(jsonData),
		DecryptedSecureJSONData: secure,
	}}
}

func defaultCtx() backend.PluginContext {
	return pluginCtx(`{}`, map[string]string{"accessKey": testAK, "secretKey": testSK})
}

func dq(refID, js string) backend.DataQuery {
	return backend.DataQuery{RefID: refID, JSON: json.RawMessage(js), TimeRange: backend.TimeRange{From: testFrom, To: testTo}}
}

const ecsQuery = `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","filter":"average","period":"1"}`

func newTestDS(f *fakeCES) *CloudEyeDatasource {
	return &CloudEyeDatasource{newMetricClient: f.factory, queryTimeout: 2 * time.Second}
}

func run(t *testing.T, ds *CloudEyeDatasource, pc backend.PluginContext, qs ...backend.DataQuery) *backend.QueryDataResponse {
	t.Helper()
	resp, err := ds.QueryData(context.Background(), &backend.QueryDataRequest{PluginContext: pc, Queries: qs})
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func values(t *testing.T, fr *data.Frame) ([]time.Time, []*float64) {
	t.Helper()
	require.Len(t, fr.Fields, 2)
	n := fr.Fields[0].Len()
	ts := make([]time.Time, n)
	vs := make([]*float64, n)
	for i := 0; i < n; i++ {
		ts[i] = fr.Fields[0].At(i).(time.Time)
		vs[i] = fr.Fields[1].At(i).(*float64)
	}
	return ts, vs
}

func TestQueryDataSingleQuery(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	resp := run(t, newTestDS(f), defaultCtx(), dq("A", ecsQuery))

	r := resp.Responses["A"]
	require.NoError(t, r.Error)
	require.Len(t, r.Frames, 1)
	fr := r.Frames[0]
	assert.Equal(t, "A", fr.RefID)
	assert.Equal(t, data.FrameTypeTimeSeriesMulti, fr.Meta.Type)
	assert.Equal(t, "cpu_util", fr.Fields[1].Name)
	assert.Equal(t, data.Labels{"namespace": "SYS.ECS", "instance_id": "i-1"}, fr.Fields[1].Labels)

	ts, vs := values(t, fr)
	// Sorted ascending despite reversed response order, millisecond precision kept.
	assert.Equal(t, []time.Time{testFrom.Add(time.Minute), testFrom.Add(2 * time.Minute)}, ts)
	assert.Equal(t, 1.0, *vs[0])
	assert.Equal(t, 2.0, *vs[1])

	require.Len(t, f.requests, 1)
	body := f.requests[0].Body
	assert.Equal(t, testFrom.UnixMilli(), body.From)
	assert.Equal(t, testTo.UnixMilli(), body.To)
	assert.Equal(t, "average", body.Filter)
	assert.Equal(t, "1", body.Period)
	assert.Equal(t, []model.MetricInfo{{Namespace: "SYS.ECS", MetricName: "cpu_util",
		Dimensions: []model.MetricsDimension{{Name: "instance_id", Value: "i-1"}}}}, body.Metrics)
	assert.Equal(t, []string{"af-south-1"}, f.regions)
}

func TestQueryDataMultipleQueriesOrderIndependent(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	resp := run(t, newTestDS(f), defaultCtx(),
		dq("A", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","filter":"max","period":"300"}`),
		dq("B", `{"region":"af-south-1","namespace":"SYS.RDS","dimstr":"rds_cluster_id:c-1,rds_instance_id:r-1","metricName":"rds001_cpu_util","filter":"max","period":"300"}`),
		dq("C", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-2","metricName":"cpu_util","filter":"max","period":"300"}`),
	)
	require.Len(t, f.requests, 1, "same region/filter/period/range should be batched")
	require.Len(t, f.requests[0].Body.Metrics, 3)

	expect := map[string]data.Labels{
		"A": {"namespace": "SYS.ECS", "instance_id": "i-1"},
		"B": {"namespace": "SYS.RDS", "rds_cluster_id": "c-1", "rds_instance_id": "r-1"},
		"C": {"namespace": "SYS.ECS", "instance_id": "i-2"},
	}
	for ref, labels := range expect {
		r := resp.Responses[ref]
		require.NoError(t, r.Error, ref)
		require.Len(t, r.Frames, 1)
		assert.Equal(t, ref, r.Frames[0].RefID)
		assert.Equal(t, labels, r.Frames[0].Fields[1].Labels, ref)
		_, vs := values(t, r.Frames[0])
		assert.Equal(t, 10.0, *vs[0], "filter max must read the max field")
	}
}

func TestQueryDataDuplicateQueriesFanOut(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	// Same metric with dimension order/whitespace differences.
	resp := run(t, newTestDS(f), defaultCtx(),
		dq("A", `{"region":"af-south-1","namespace":"SYS.RDS","dimstr":"rds_cluster_id:c-1,rds_instance_id:r-1","metricName":"m"}`),
		dq("B", `{"region":"af-south-1","namespace":"SYS.RDS","dimstr":" rds_instance_id:r-1 , rds_cluster_id:c-1","metricName":"m"}`),
	)
	require.Len(t, f.requests, 1)
	assert.Len(t, f.requests[0].Body.Metrics, 1, "identical metric requested once")
	for _, ref := range []string{"A", "B"} {
		require.NoError(t, resp.Responses[ref].Error)
		require.Len(t, resp.Responses[ref].Frames, 1)
		assert.Equal(t, ref, resp.Responses[ref].Frames[0].RefID)
		assert.Equal(t, 2, resp.Responses[ref].Frames[0].Rows())
	}
}

func TestQueryDataDefaultsAndNumericPeriod(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	resp := run(t, newTestDS(f), defaultCtx(),
		dq("A", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util"}`),
		dq("B", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","period":300,"filter":null}`),
	)
	require.NoError(t, resp.Responses["A"].Error)
	require.NoError(t, resp.Responses["B"].Error)
	require.Len(t, f.requests, 2)
	got := map[string]string{}
	for _, r := range f.requests {
		got[r.Body.Period] = r.Body.Filter
	}
	assert.Equal(t, map[string]string{"1": "average", "300": "average"}, got)
}

func TestQueryDataNoData(t *testing.T) {
	cases := map[string]func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error){
		"nil metrics": func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			return &model.BatchListMetricDataResponse{}, nil
		},
		"empty metrics": func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			return &model.BatchListMetricDataResponse{Metrics: &[]model.BatchMetricData{}}, nil
		},
		"empty datapoints": func(_ string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			m := req.Body.Metrics[0]
			return &model.BatchListMetricDataResponse{Metrics: &[]model.BatchMetricData{{
				Namespace: strp(m.Namespace), MetricName: m.MetricName, Dimensions: &m.Dimensions,
			}}}, nil
		},
		"only null values": func(_ string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			m := req.Body.Metrics[0]
			return &model.BatchListMetricDataResponse{Metrics: &[]model.BatchMetricData{{
				Namespace: strp(m.Namespace), MetricName: m.MetricName, Dimensions: &m.Dimensions,
				Datapoints: []model.DatapointForBatchMetric{
					{Timestamp: req.Body.From},
					{Timestamp: req.Body.From + 60000, Max: f64(1)}, // other statistic only
				},
			}}}, nil
		},
		"only invalid timestamps": func(_ string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			m := req.Body.Metrics[0]
			return &model.BatchListMetricDataResponse{Metrics: &[]model.BatchMetricData{{
				Namespace: strp(m.Namespace), MetricName: m.MetricName, Dimensions: &m.Dimensions,
				Datapoints: []model.DatapointForBatchMetric{{Timestamp: 0, Average: f64(1)}},
			}}}, nil
		},
	}
	for name, respond := range cases {
		t.Run(name, func(t *testing.T) {
			// Filter average with period 300: other statistics are never substituted.
			resp := run(t, newTestDS(&fakeCES{respond: respond}), defaultCtx(),
				dq("A", strings.Replace(ecsQuery, `"period":"1"`, `"period":"300"`, 1)))
			r, ok := resp.Responses["A"]
			require.True(t, ok, "ref ID must be present in the response")
			require.NoError(t, r.Error)
			assert.Equal(t, backend.StatusOK, r.Status)
			// No frames: Grafana alerting evaluates this as No Data. A zero-row
			// labelled frame would reduce to a labelled NaN series (Normal).
			assert.Empty(t, r.Frames)
		})
	}
}

func TestQueryDataNullAndMalformedDatapoints(t *testing.T) {
	respond := func(_ string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
		dims := []model.MetricsDimension{{Name: "instance_id", Value: "i-1"}}
		other := []model.MetricsDimension{{Name: "instance_id", Value: "not-requested"}}
		from := req.Body.From
		return &model.BatchListMetricDataResponse{Metrics: &[]model.BatchMetricData{
			// Unrequested metric: ignored.
			{Namespace: strp("SYS.ECS"), MetricName: "cpu_util", Dimensions: &other,
				Datapoints: []model.DatapointForBatchMetric{{Timestamp: from, Max: f64(999)}}},
			// Metric without dimensions: ignored.
			{Namespace: strp("SYS.ECS"), MetricName: "cpu_util",
				Datapoints: []model.DatapointForBatchMetric{{Timestamp: from, Max: f64(999)}}},
			// Missing namespace, matched by name + dimensions; split across two entries.
			{MetricName: "cpu_util", Dimensions: &dims, Datapoints: []model.DatapointForBatchMetric{
				{Timestamp: from + 180000, Max: f64(3)},
				{Timestamp: 0, Max: f64(999)},              // invalid timestamp: dropped
				{Timestamp: from + 60000, Average: f64(5)}, // max missing: null
			}},
			{Namespace: strp("SYS.ECS"), MetricName: "cpu_util", Dimensions: &dims, Datapoints: []model.DatapointForBatchMetric{
				{Timestamp: from + 120000, Max: f64(2)},
				{Timestamp: from + 180000, Max: f64(4)}, // duplicate timestamp: last wins
			}},
		}}, nil
	}
	resp := run(t, newTestDS(&fakeCES{respond: respond}), defaultCtx(),
		dq("A", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","filter":"max","period":"300"}`))
	r := resp.Responses["A"]
	require.NoError(t, r.Error)
	ts, vs := values(t, r.Frames[0])
	assert.Equal(t, []time.Time{testFrom.Add(time.Minute), testFrom.Add(2 * time.Minute), testFrom.Add(3 * time.Minute)}, ts)
	assert.Nil(t, vs[0])
	assert.Equal(t, 2.0, *vs[1])
	assert.Equal(t, 4.0, *vs[2])
}

func TestQueryDataRawPeriodFallsBackToAverage(t *testing.T) {
	respond := func(_ string, req *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
		m := req.Body.Metrics[0]
		return &model.BatchListMetricDataResponse{Metrics: &[]model.BatchMetricData{{
			Namespace: strp(m.Namespace), MetricName: m.MetricName, Dimensions: &m.Dimensions,
			Datapoints: []model.DatapointForBatchMetric{{Timestamp: req.Body.From, Average: f64(42)}},
		}}}, nil
	}
	resp := run(t, newTestDS(&fakeCES{respond: respond}), defaultCtx(),
		dq("A", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","filter":"max","period":"1"}`),
		dq("B", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","filter":"max","period":"300"}`))
	_, vs := values(t, resp.Responses["A"].Frames[0])
	assert.Equal(t, 42.0, *vs[0], "period 1 ignores filter in CES; use average")
	require.NoError(t, resp.Responses["B"].Error)
	assert.Empty(t, resp.Responses["B"].Frames, "aggregated period must not substitute another statistic; all-null is No Data")
}

func TestQueryDataValidation(t *testing.T) {
	cases := []struct {
		name, js, want string
		tr             *backend.TimeRange
	}{
		{"empty model", ``, "query model is empty", nil},
		{"invalid json", `{`, "invalid query model", nil},
		{"wrong type", `{"region":{"a":1}}`, "invalid query model", nil},
		{"missing all", `{}`, "missing required query field(s): namespace, dimstr, metricName", nil},
		{"missing metric", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1"}`, "metricName", nil},
		{"variable region", `{"region":"$region","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m"}`, "dashboard template variable", nil},
		{"variable braces", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:${ecs}","metricName":"m"}`, "dimstr \"instance_id:${ecs}\" contains a dashboard template variable", nil},
		{"variable brackets", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"[[metric]]"}`, "template variable", nil},
		{"variable period", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","period":"$period"}`, "period \"$period\"", nil},
		{"variable filter", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","filter":"$filter"}`, "filter \"$filter\"", nil},
		{"bad filter", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","filter":"p99"}`, "unsupported filter", nil},
		{"bad period", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","period":"5"}`, "unsupported period", nil},
		{"dim no colon", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id,i-1","metricName":"m"}`, "must use the form name:value", nil},
		{"dim empty value", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:","metricName":"m"}`, "non-empty name and value", nil},
		{"dim only commas", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":" , ","metricName":"m"}`, "at least one", nil},
		{"dim duplicate", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"a:1,a:2","metricName":"m"}`, "more than once", nil},
		{"dim too many", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"a:1,b:2,c:3,d:4,e:5","metricName":"m"}`, "at most 4", nil},
		{"inverted range", ecsQuery, "invalid time range", &backend.TimeRange{From: testTo, To: testFrom}},
		{"zero range", ecsQuery, "invalid time range", &backend.TimeRange{}},
		{"raw span cap", ecsQuery, "exceeds the CES maximum of 4h0m0s for period 1", &backend.TimeRange{From: testTo.Add(-5 * time.Hour), To: testTo}},
		{"template placeholder region", `{"region":"-","namespace":"SYS.ECS","dimstr":"instance_id:00000000-0000-0000-0000-000000000000","metricName":"cpu_util","filter":"average","period":"1"}`, "dashboard template placeholder", nil},
		{"period 60 unsupported", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","period":"60"}`, "unsupported period \"60\"; expected one of 1, 300, 1200, 3600, 14400, 86400", nil},
		{"period 60 numeric", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","period":60}`, "unsupported period", nil},
		{"300 span cap", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","period":"300"}`, "exceeds the CES maximum of 24h0m0s for period 300", &backend.TimeRange{From: testTo.Add(-25 * time.Hour), To: testTo}},
		{"86400 span cap", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m","period":"86400"}`, "for period 86400", &backend.TimeRange{From: testTo.Add(-181 * 24 * time.Hour), To: testTo}},
	}

	f := &fakeCES{respond: echoResponse}
	ds := newTestDS(f)
	qs := []backend.DataQuery{dq("OK", ecsQuery)}
	for i, c := range cases {
		q := dq(fmt.Sprintf("Q%d", i), c.js)
		if c.tr != nil {
			q.TimeRange = *c.tr
		}
		qs = append(qs, q)
	}
	resp := run(t, ds, defaultCtx(), qs...)

	for i, c := range cases {
		r := resp.Responses[fmt.Sprintf("Q%d", i)]
		if assert.Error(t, r.Error, c.name) {
			assert.Contains(t, r.Error.Error(), c.want, c.name)
			assert.Equal(t, backend.StatusBadRequest, r.Status, c.name)
		}
		assert.Empty(t, r.Frames, c.name)
	}
	// Valid query in the same request still succeeds; invalid ones never reach CES.
	require.NoError(t, resp.Responses["OK"].Error)
	require.Len(t, f.requests, 1)
	assert.Len(t, f.requests[0].Body.Metrics, 1)
}

func TestQueryDataDimensionValueWithColon(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	resp := run(t, newTestDS(f), defaultCtx(),
		dq("A", `{"region":"af-south-1","namespace":"SYS.ELB","dimstr":"lbaas_listener_id:l-1,lbaas_instance_id:arn:x","metricName":"m"}`))
	require.NoError(t, resp.Responses["A"].Error)
	assert.Equal(t, []model.MetricsDimension{{Name: "lbaas_instance_id", Value: "arn:x"}, {Name: "lbaas_listener_id", Value: "l-1"}},
		f.requests[0].Body.Metrics[0].Dimensions)
}

func TestQueryDataSingleRegionModeRegionMismatch(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	pc := pluginCtx(`{"projectId":"p","cesEndpoint":"https://ces.example.test","region":"af-south-1"}`,
		map[string]string{"accessKey": testAK, "secretKey": testSK})
	resp := run(t, newTestDS(f), pc,
		dq("A", ecsQuery),
		dq("B", strings.Replace(ecsQuery, "af-south-1", "cn-east-3", 1)))
	require.NoError(t, resp.Responses["A"].Error)
	require.Error(t, resp.Responses["B"].Error)
	assert.Contains(t, resp.Responses["B"].Error.Error(), "does not match the datasource region")
}

func TestQueryDataSettingsErrors(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	ds := newTestDS(f)
	cases := map[string]struct {
		pc   backend.PluginContext
		want string
	}{
		"nil settings":   {backend.PluginContext{}, "datasource settings are missing"},
		"bad json":       {pluginCtx(`{`, map[string]string{"accessKey": testAK, "secretKey": testSK}), "invalid datasource settings"},
		"missing keys":   {pluginCtx(`{}`, nil), "access key and secret key must be configured"},
		"missing secret": {pluginCtx(`{}`, map[string]string{"accessKey": testAK}), "access key and secret key must be configured"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			resp := run(t, ds, c.pc, dq("A", ecsQuery), dq("B", ecsQuery))
			for _, ref := range []string{"A", "B"} {
				require.Error(t, resp.Responses[ref].Error)
				assert.Contains(t, resp.Responses[ref].Error.Error(), c.want)
				assert.NotContains(t, resp.Responses[ref].Error.Error(), testSK)
			}
		})
	}
	assert.Empty(t, f.requests)
}

func TestQueryDataEmptyRequest(t *testing.T) {
	ds := newTestDS(&fakeCES{respond: echoResponse})
	resp, err := ds.QueryData(context.Background(), &backend.QueryDataRequest{PluginContext: defaultCtx()})
	require.NoError(t, err)
	assert.Empty(t, resp.Responses)
	resp, err = ds.QueryData(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, resp.Responses)
}

func TestQueryDataCESErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status backend.Status
		want   string
	}{
		{"unauthorized", &sdkerr.ServiceResponseError{StatusCode: 401, ErrorCode: "APIGW.0301", ErrorMessage: "Incorrect IAM authentication information"}, backend.StatusUnauthorized, "APIGW.0301"},
		{"forbidden", &sdkerr.ServiceResponseError{StatusCode: 403, ErrorCode: "CES.0001"}, backend.StatusForbidden, "CES.0001"},
		{"throttled", &sdkerr.ServiceResponseError{StatusCode: 429, ErrorCode: "APIGW.0308"}, backend.StatusTooManyRequests, "APIGW.0308"},
		{"bad request value", sdkerr.ServiceResponseError{StatusCode: 400, ErrorCode: "ces.0014"}, backend.StatusBadRequest, "ces.0014"},
		{"server error", &sdkerr.ServiceResponseError{StatusCode: 500}, backend.StatusBadGateway, "CES query failed"},
		{"http client timeout", &url.Error{Op: "Post", URL: "https://ces.example.test", Err: timeoutNetError{}}, backend.StatusTimeout, "timed out"},
		{"wrapped http timeout", fmt.Errorf("send: %w", &url.Error{Op: "Post", URL: "https://ces.example.test", Err: timeoutNetError{}}), backend.StatusTimeout, "timed out"},
		{"non-timeout net error", &url.Error{Op: "Post", URL: "https://ces.example.test", Err: errors.New("connection reset")}, backend.StatusBadGateway, "connection reset"},
		{"connection", sdkerr.NewConnectionError("connection refused"), backend.StatusBadGateway, "connection refused"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeCES{respond: func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
				return nil, c.err
			}}
			resp := run(t, newTestDS(f), defaultCtx(), dq("A", ecsQuery), dq("B", strings.Replace(ecsQuery, "i-1", "i-2", 1)))
			for _, ref := range []string{"A", "B"} {
				r := resp.Responses[ref]
				require.Error(t, r.Error)
				assert.Equal(t, c.status, r.Status)
				assert.Contains(t, r.Error.Error(), c.want)
				assert.Empty(t, r.Frames)
			}
		})
	}
}

func TestQueryDataNilResponseAndPanic(t *testing.T) {
	for name, respond := range map[string]func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error){
		"nil response": func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			return nil, nil
		},
		"panic": func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
			panic("boom")
		},
	} {
		t.Run(name, func(t *testing.T) {
			resp := run(t, newTestDS(&fakeCES{respond: respond}), defaultCtx(), dq("A", ecsQuery))
			require.Error(t, resp.Responses["A"].Error)
			assert.Equal(t, backend.StatusBadGateway, resp.Responses["A"].Status)
		})
	}
}

func TestQueryDataTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	f := &fakeCES{respond: func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
		<-release
		return nil, errors.New("too late")
	}}
	ds := &CloudEyeDatasource{newMetricClient: f.factory, queryTimeout: 50 * time.Millisecond}
	start := time.Now()
	resp := run(t, ds, defaultCtx(), dq("A", ecsQuery))
	assert.Less(t, time.Since(start), time.Second)
	require.Error(t, resp.Responses["A"].Error)
	assert.Equal(t, backend.StatusTimeout, resp.Responses["A"].Status)
	assert.Contains(t, resp.Responses["A"].Error.Error(), "timed out")
}

func TestQueryDataCancelledContext(t *testing.T) {
	f := &fakeCES{respond: func(string, *model.BatchListMetricDataRequest) (*model.BatchListMetricDataResponse, error) {
		time.Sleep(200 * time.Millisecond)
		return &model.BatchListMetricDataResponse{}, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := newTestDS(f).QueryData(ctx, &backend.QueryDataRequest{PluginContext: defaultCtx(), Queries: []backend.DataQuery{dq("A", ecsQuery)}})
	require.NoError(t, err)
	require.Error(t, resp.Responses["A"].Error)
	assert.Equal(t, backend.StatusTimeout, resp.Responses["A"].Status)
}

func TestQueryDataPartialFailureAcrossRegions(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	ds := &CloudEyeDatasource{queryTimeout: 2 * time.Second, newMetricClient: func(cfg *CloudEyeSettings, d time.Duration) (metricDataClient, error) {
		switch cfg.Region {
		case "xx-unknown-1":
			return nil, validationError("unsupported CES region %q", cfg.Region)
		case "ap-southeast-1":
			// e.g. IAM project lookup failure while building the client
			return nil, errors.New("failed to create CES client: iam: connection refused")
		case "ap-southeast-2":
			panic("sdk panic")
		}
		return f.factory(cfg, d)
	}}
	resp := run(t, ds, defaultCtx(),
		dq("A", ecsQuery),
		dq("B", strings.Replace(ecsQuery, "af-south-1", "xx-unknown-1", 1)),
		dq("C", strings.Replace(ecsQuery, "af-south-1", "cn-east-3", 1)),
		dq("D", strings.Replace(ecsQuery, "af-south-1", "ap-southeast-1", 1)),
		dq("E", strings.Replace(ecsQuery, "af-south-1", "ap-southeast-2", 1)),
	)
	require.NoError(t, resp.Responses["A"].Error)
	require.NoError(t, resp.Responses["C"].Error)
	require.Error(t, resp.Responses["B"].Error)
	assert.Equal(t, backend.StatusBadRequest, resp.Responses["B"].Status)
	assert.Contains(t, resp.Responses["B"].Error.Error(), "unsupported CES region")
	require.Error(t, resp.Responses["D"].Error)
	assert.Equal(t, backend.StatusBadGateway, resp.Responses["D"].Status, "client construction network failure is not a 400")
	assert.Contains(t, resp.Responses["D"].Error.Error(), "connection refused")
	require.Error(t, resp.Responses["E"].Error)
	assert.Equal(t, backend.StatusBadGateway, resp.Responses["E"].Status)
	assert.ElementsMatch(t, []string{"af-south-1", "cn-east-3"}, f.regions)
	assert.Len(t, f.requests, 2)
}

func TestQueryDataSeparateTimeRangesAreSeparateRequests(t *testing.T) {
	f := &fakeCES{respond: echoResponse}
	b := dq("B", ecsQuery)
	b.TimeRange = backend.TimeRange{From: testFrom.Add(-time.Hour), To: testTo}
	resp := run(t, newTestDS(f), defaultCtx(), dq("A", ecsQuery), b)
	require.NoError(t, resp.Responses["A"].Error)
	require.NoError(t, resp.Responses["B"].Error)
	require.Len(t, f.requests, 2)
	assert.Equal(t, []string{"af-south-1"}, f.regions, "one client per region")
}

func TestPlanBatchesRespectsCESLimits(t *testing.T) {
	mk := func(n int, period string, span time.Duration) []*parsedQuery {
		var qs []*parsedQuery
		for i := 0; i < n; i++ {
			qs = append(qs, &parsedQuery{RefID: fmt.Sprintf("R%d", i), Region: "r", Namespace: "SYS.ECS", MetricName: "m",
				Dimensions: []model.MetricsDimension{{Name: "instance_id", Value: fmt.Sprintf("i-%d", i)}},
				Filter:     "average", Period: period, From: testTo.Add(-span).UnixMilli(), To: testTo.UnixMilli()})
		}
		return qs
	}
	sizes := func(bs []*batch) []int {
		var out []int
		for _, b := range bs {
			out = append(out, len(b.metrics))
			pts := int64(len(b.metrics)) * estimatedDatapoints(b.period, b.from, b.to)
			assert.LessOrEqual(t, pts, int64(maxDatapointsPerRequest))
		}
		return out
	}
	// 10 minutes at 300s: 3 points per metric -> capped by the 500 metric limit.
	assert.Equal(t, []int{500, 100}, sizes(planBatches(mk(600, "300", 10*time.Minute))))
	// 4 hours raw data (~60s interval): 241 points per metric -> 12 metrics per request.
	assert.Equal(t, []int{12, 12, 1}, sizes(planBatches(mk(25, "1", 4*time.Hour))))

	// Every RefID is covered exactly once.
	seen := map[string]int{}
	for _, b := range planBatches(mk(25, "1", 4*time.Hour)) {
		for _, refs := range b.refIDs {
			for _, r := range refs {
				seen[r]++
			}
		}
	}
	assert.Len(t, seen, 25)
}

func TestNewSafeCESClientUnknownRegionDoesNotPanic(t *testing.T) {
	cfg := &CloudEyeSettings{AK: testAK, SK: testSK, Region: "xx-unknown-1"}
	require.NotPanics(t, func() {
		c, err := newSafeCESClient(cfg, time.Second)
		require.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), `unsupported CES region "xx-unknown-1"`)
	})

	// Through QueryData with the real client factory.
	ds := &CloudEyeDatasource{queryTimeout: 2 * time.Second}
	resp := run(t, ds, defaultCtx(), dq("A", strings.Replace(ecsQuery, "af-south-1", "xx-unknown-1", 1)))
	require.Error(t, resp.Responses["A"].Error)
	assert.Equal(t, backend.StatusBadRequest, resp.Responses["A"].Status)
	assert.Contains(t, resp.Responses["A"].Error.Error(), "unsupported CES region")
}

// End-to-end through the real Huawei Cloud SDK client (request signing, JSON
// decoding) against a local fake CES endpoint in Specific Region Mode.
func TestQueryDataWithRealSDKAgainstFakeEndpoint(t *testing.T) {
	var calls int32
	var gotBody model.BatchListMetricDataRequestBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		if r.Method != http.MethodPost || r.URL.Path != "/V1.0/test-project/batch-query-metric-data" {
			http.Error(w, `{"error_code":"CES.404","error_msg":"not found"}`, http.StatusNotFound)
			return
		}
		if !strings.Contains(r.Header.Get("Authorization"), "Access="+testAK) {
			http.Error(w, `{"error_code":"APIGW.0301","error_msg":"auth"}`, http.StatusUnauthorized)
			return
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		m := gotBody.Metrics[0]
		dims, _ := json.Marshal(m.Dimensions)
		w.Header().Set("Content-Type", "application/json")
		// Unexpected extra fields and a null datapoint value must be tolerated.
		fmt.Fprintf(w, `{"metrics":[{"namespace":%q,"metric_name":%q,"dimensions":%s,"unit":"%%","extra":1,
			"datapoints":[{"average":null,"timestamp":%d},{"average":12.5,"timestamp":%d}]}]}`,
			m.Namespace, m.MetricName, dims, gotBody.From+60000, gotBody.From)
	}))
	defer srv.Close()

	pc := pluginCtx(fmt.Sprintf(`{"projectId":"test-project","cesEndpoint":%q,"region":"af-south-1"}`, srv.URL),
		map[string]string{"accessKey": testAK, "secretKey": testSK})
	ds := &CloudEyeDatasource{queryTimeout: 5 * time.Second}
	resp := run(t, ds, pc, dq("A", `{"region":"af-south-1","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util","filter":"average","period":"300"}`))

	r := resp.Responses["A"]
	require.NoError(t, r.Error)
	assert.EqualValues(t, 1, atomic.LoadInt32(&calls))
	assert.Equal(t, "300", gotBody.Period)
	assert.Equal(t, testFrom.UnixMilli(), gotBody.From)
	ts, vs := values(t, r.Frames[0])
	assert.Equal(t, []time.Time{testFrom, testFrom.Add(time.Minute)}, ts)
	assert.Equal(t, 12.5, *vs[0])
	assert.Nil(t, vs[1])

	// A CES error status is surfaced per query.
	pc401 := pluginCtx(fmt.Sprintf(`{"projectId":"test-project","cesEndpoint":%q}`, srv.URL),
		map[string]string{"accessKey": "OTHERKEY", "secretKey": testSK})
	resp = run(t, ds, pc401, dq("A", ecsQuery))
	require.Error(t, resp.Responses["A"].Error)
	assert.Equal(t, backend.StatusUnauthorized, resp.Responses["A"].Status)
	assert.NotContains(t, resp.Responses["A"].Error.Error(), testSK)
}

// The dashboard /metric-data request parsing is unchanged.
func TestDashboardMetricDataRequestParsingUnchanged(t *testing.T) {
	cfg := &CloudEyeSettings{Region: "configured"}
	refIDs, req := buildCustomBatchQueryParams([]byte(`{"metrics":[{"namespace":"SYS.ECS","metric_name":"cpu_util",
		"dimensions":[{"name":"instance_id","value":"i-1"}]}],"refIDs":["A"],"from":1,"to":2,"filter":"average","period":"1","region":"cn-east-3"}`), cfg)
	assert.Equal(t, []string{"A"}, refIDs)
	assert.Equal(t, "cn-east-3", cfg.Region)
	assert.Equal(t, "cpu_util", req.Body.Metrics[0].MetricName)
}

func TestPluginJSONEnablesAlerting(t *testing.T) {
	b, err := os.ReadFile("../../src/plugin.json")
	require.NoError(t, err)
	var manifest struct {
		ID       string `json:"id"`
		Backend  bool   `json:"backend"`
		Alerting bool   `json:"alerting"`
	}
	require.NoError(t, json.Unmarshal(b, &manifest))
	assert.Equal(t, "huawei-cloudeye-grafana", manifest.ID)
	assert.True(t, manifest.Backend)
	assert.True(t, manifest.Alerting)
}

func TestQueryDataMissingRegionDefaults(t *testing.T) {
	noRegion := `{"namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util"}`
	emptyRegion := `{"region":"  ","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"cpu_util"}`
	secure := map[string]string{"accessKey": testAK, "secretKey": testSK}
	cases := []struct {
		name, settings, wantRegion string
	}{
		{"huawei cloud mode uses editor default", `{}`, "cn-east-3"},
		{"huawei cloud mode ignores configured region", `{"region":"af-south-1"}`, "cn-east-3"},
		{"specific region mode uses configured region", `{"projectId":"p","cesEndpoint":"https://ces.example.test","region":"af-south-1"}`, "af-south-1"},
		{"specific region mode without region", `{"projectId":"p","cesEndpoint":"https://ces.example.test"}`, "cn-east-3"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeCES{respond: echoResponse}
			resp := run(t, newTestDS(f), pluginCtx(c.settings, secure), dq("A", noRegion), dq("B", emptyRegion))
			require.NoError(t, resp.Responses["A"].Error)
			require.NoError(t, resp.Responses["B"].Error)
			require.Len(t, resp.Responses["A"].Frames, 1)
			assert.Equal(t, []string{c.wantRegion}, f.regions)
			require.Len(t, f.requests, 1, "both queries share the defaulted region")
		})
	}

	// The defaulted region is still validated: a template variable is rejected.
	resp := run(t, newTestDS(&fakeCES{respond: echoResponse}), defaultCtx(),
		dq("A", `{"region":"${region}","namespace":"SYS.ECS","dimstr":"instance_id:i-1","metricName":"m"}`))
	require.Error(t, resp.Responses["A"].Error)
	assert.Contains(t, resp.Responses["A"].Error.Error(), "template variable")
}

func TestBuildClientCapsTimeoutAtContextDeadline(t *testing.T) {
	var got time.Duration
	factory := func(_ *CloudEyeSettings, d time.Duration) (metricDataClient, error) {
		got = d
		return &fakeRegionClient{f: &fakeCES{respond: echoResponse}}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := buildClient(ctx, factory, &CloudEyeSettings{}, 30*time.Second)
	require.NoError(t, err)
	assert.Greater(t, got, time.Duration(0))
	assert.LessOrEqual(t, got, 200*time.Millisecond)

	_, err = buildClient(context.Background(), factory, &CloudEyeSettings{}, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, 30*time.Second, got, "no deadline keeps the configured timeout")

	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	_, err = buildClient(expired, factory, &CloudEyeSettings{}, 30*time.Second)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// A slow CES endpoint is cut off by the derived SDK HTTP timeout or the query
// deadline; either way the query reports 504, never 502.
func TestQueryDataRealSDKSlowEndpointTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	pc := pluginCtx(fmt.Sprintf(`{"projectId":"test-project","cesEndpoint":%q}`, srv.URL),
		map[string]string{"accessKey": testAK, "secretKey": testSK})
	ds := &CloudEyeDatasource{queryTimeout: 200 * time.Millisecond}
	start := time.Now()
	resp := run(t, ds, pc, dq("A", ecsQuery))
	assert.Less(t, time.Since(start), 2*time.Second)
	require.Error(t, resp.Responses["A"].Error)
	assert.Equal(t, backend.StatusTimeout, resp.Responses["A"].Status)
}

// The alerting client verifies TLS certificates (the legacy dashboard client
// does not). An untrusted certificate must fail before any request is served.
func TestQueryDataRealSDKVerifiesTLS(t *testing.T) {
	var calls int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"metrics":[]}`))
	}))
	defer srv.Close()
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)

	pc := pluginCtx(fmt.Sprintf(`{"projectId":"test-project","cesEndpoint":%q}`, srv.URL),
		map[string]string{"accessKey": testAK, "secretKey": testSK})
	resp := run(t, &CloudEyeDatasource{queryTimeout: 5 * time.Second}, pc, dq("A", ecsQuery))
	require.Error(t, resp.Responses["A"].Error)
	assert.Equal(t, backend.StatusBadGateway, resp.Responses["A"].Status)
	assert.Contains(t, resp.Responses["A"].Error.Error(), "certificate")
	assert.EqualValues(t, 0, atomic.LoadInt32(&calls))
}
