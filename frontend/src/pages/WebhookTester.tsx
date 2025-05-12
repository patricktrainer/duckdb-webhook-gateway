import React, { useState, useEffect } from 'react';
import { useQuery } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Grid,
  Typography,
  CircularProgress,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  Paper,
  Divider,
  SelectChangeEvent,
  Tabs,
  Tab,
} from '@mui/material';
import SendIcon from '@mui/icons-material/Send';
import { useSnackbar } from 'notistack';
import { webhookApi, testApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import CodeEditor from '../components/CodeEditor';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`webhook-tabpanel-${index}`}
      aria-labelledby={`webhook-tab-${index}`}
      {...other}
    >
      {value === index && (
        <Box sx={{ p: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

function a11yProps(index: number) {
  return {
    id: `webhook-tab-${index}`,
    'aria-controls': `webhook-tabpanel-${index}`,
  };
}

const WebhookTester: React.FC = () => {
  const { enqueueSnackbar } = useSnackbar();
  const [selectedWebhookId, setSelectedWebhookId] = useState<string>('');
  const [payload, setPayload] = useState<string>('{\n  "type": "PushEvent",\n  "repository": {\n    "id": 123456,\n    "name": "webhook-gateway",\n    "full_name": "user/webhook-gateway"\n  },\n  "sender": {\n    "login": "john_doe",\n    "id": 12345\n  },\n  "commit": {\n    "id": "abcdef1234567890",\n    "message": "Fix bug in login page [PROJ-123]"\n  }\n}');
  const [loading, setLoading] = useState<boolean>(false);
  const [response, setResponse] = useState<any>(null);
  const [transformedData, setTransformedData] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [tabValue, setTabValue] = useState(0);

  const { data: webhooks, isLoading } = useQuery('webhooks', webhookApi.getAll);

  const selectedWebhook = webhooks?.find(w => w.id === selectedWebhookId);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handlePayloadChange = (value: string | undefined) => {
    setPayload(value || '');
  };

  const handleWebhookChange = (event: SelectChangeEvent) => {
    setSelectedWebhookId(event.target.value);
    setResponse(null);
    setTransformedData(null);
    setError(null);
  };

  const handleTestWebhook = async () => {
    if (!selectedWebhook) {
      enqueueSnackbar('Please select a webhook', { variant: 'error' });
      return;
    }

    let parsedPayload;
    try {
      parsedPayload = JSON.parse(payload);
    } catch (err) {
      enqueueSnackbar('Invalid JSON payload', { variant: 'error' });
      return;
    }

    setLoading(true);
    setResponse(null);
    setTransformedData(null);
    setError(null);

    try {
      const result = await testApi.sendWebhook(selectedWebhook.source_path, parsedPayload);
      setResponse(result);

      // If the webhook test was successful and we received an event ID, fetch the transformed data
      if (result && result.event_id) {
        try {
          const transformedResult = await testApi.getTransformedEvent(result.event_id);
          setTransformedData(transformedResult);
        } catch (transformError) {
          console.error('Error fetching transformed data:', transformError);
          // Don't set this as an error since the webhook test itself succeeded
        }
      }

      enqueueSnackbar('Webhook test successful', { variant: 'success' });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error occurred');
      enqueueSnackbar('Webhook test failed', { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <PageHeader title="Webhook Tester" />

      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Test Your Webhooks
          </Typography>
          <Typography variant="body2" color="textSecondary" paragraph>
            Send test payloads to your registered webhooks and see how they are processed.
          </Typography>

          <Grid container spacing={3}>
            <Grid item xs={12}>
              <FormControl fullWidth sx={{ mb: 3 }}>
                <InputLabel id="webhook-select-label">Select Webhook</InputLabel>
                <Select
                  labelId="webhook-select-label"
                  id="webhook-select"
                  value={selectedWebhookId}
                  label="Select Webhook"
                  onChange={handleWebhookChange}
                  disabled={isLoading}
                >
                  {webhooks?.map((webhook) => (
                    <MenuItem key={webhook.id} value={webhook.id}>
                      {webhook.source_path} ({webhook.owner})
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>
            </Grid>

            {selectedWebhook && (
              <>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Source Path"
                    value={selectedWebhook.source_path}
                    InputProps={{ readOnly: true }}
                  />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TextField
                    fullWidth
                    label="Destination URL"
                    value={selectedWebhook.destination_url}
                    InputProps={{ readOnly: true }}
                  />
                </Grid>
              </>
            )}

            <Grid item xs={12}>
              <Typography variant="subtitle1" gutterBottom>
                JSON Payload
              </Typography>
              <CodeEditor
                language="json"
                value={payload}
                onChange={handlePayloadChange}
              />
            </Grid>

            <Grid item xs={12}>
              <Box display="flex" justifyContent="flex-end">
                <Button
                  variant="contained"
                  color="primary"
                  startIcon={<SendIcon />}
                  onClick={handleTestWebhook}
                  disabled={loading || !selectedWebhookId}
                >
                  {loading ? <CircularProgress size={24} /> : 'Send Test Webhook'}
                </Button>
              </Box>
            </Grid>
          </Grid>
        </CardContent>
      </Card>

      {(response || error || transformedData) && (
        <Paper sx={{ p: 3 }}>
          <Typography variant="h6" gutterBottom>
            Test Results
          </Typography>

          {error && (
            <Box sx={{ bgcolor: '#ffebee', p: 2, borderRadius: 1, mb: 2 }}>
              <Typography color="error" variant="body1">
                Error: {error}
              </Typography>
            </Box>
          )}

          {(response || transformedData) && (
            <>
              <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
                <Tabs value={tabValue} onChange={handleTabChange} aria-label="webhook result tabs">
                  <Tab label="Response" {...a11yProps(0)} />
                  {transformedData && <Tab label="Raw Payload" {...a11yProps(1)} />}
                  {transformedData?.transformed && <Tab label="Transformed Data" {...a11yProps(2)} />}
                  {transformedData?.transformed && <Tab label="Response Details" {...a11yProps(3)} />}
                </Tabs>
              </Box>

              <TabPanel value={tabValue} index={0}>
                <Typography variant="subtitle2" gutterBottom>
                  API Response
                </Typography>
                <Box sx={{ bgcolor: '#f5f5f5', p: 2, borderRadius: 1, mb: 2 }}>
                  <pre>{JSON.stringify(response, null, 2)}</pre>
                </Box>

                <Typography variant="subtitle2" gutterBottom>
                  Event ID
                </Typography>
                <Typography variant="body1" paragraph>
                  {response?.event_id || 'N/A'}
                </Typography>
              </TabPanel>

              {transformedData && (
                <TabPanel value={tabValue} index={1}>
                  <Typography variant="subtitle2" gutterBottom>
                    Original Webhook Payload
                  </Typography>
                  <Box sx={{ bgcolor: '#f5f5f5', p: 2, borderRadius: 1 }}>
                    <pre>{JSON.stringify(transformedData.raw_payload || {}, null, 2)}</pre>
                  </Box>
                </TabPanel>
              )}

              {transformedData?.transformed && (
                <TabPanel value={tabValue} index={2}>
                  <Typography variant="subtitle2" gutterBottom>
                    Transformed Payload
                  </Typography>
                  <Typography variant="body2" color="textSecondary" paragraph>
                    This is the data after applying your SQL transformation:
                  </Typography>
                  <Box sx={{ bgcolor: '#f5f5f5', p: 2, borderRadius: 1 }}>
                    <pre>{JSON.stringify(transformedData.transformed.payload || {}, null, 2)}</pre>
                  </Box>
                </TabPanel>
              )}

              {transformedData?.transformed && (
                <TabPanel value={tabValue} index={3}>
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <Typography variant="subtitle2" gutterBottom>
                        Destination URL
                      </Typography>
                      <Typography variant="body1" paragraph>
                        {transformedData.transformed.destination_url || 'N/A'}
                      </Typography>
                    </Grid>

                    <Grid item xs={12} md={6}>
                      <Typography variant="subtitle2" gutterBottom>
                        Delivery Status
                      </Typography>
                      <Typography
                        variant="body1"
                        paragraph
                        sx={{
                          color: transformedData.transformed.success ? 'success.main' : 'error.main',
                          fontWeight: 'bold'
                        }}
                      >
                        {transformedData.transformed.success ? 'Success' : 'Failed'}
                      </Typography>
                    </Grid>

                    <Grid item xs={12} md={6}>
                      <Typography variant="subtitle2" gutterBottom>
                        Response Code
                      </Typography>
                      <Typography variant="body1" paragraph>
                        {transformedData.transformed.response_code || 'N/A'}
                      </Typography>
                    </Grid>

                    <Grid item xs={12} md={6}>
                      <Typography variant="subtitle2" gutterBottom>
                        Timestamp
                      </Typography>
                      <Typography variant="body1" paragraph>
                        {transformedData.transformed.timestamp || 'N/A'}
                      </Typography>
                    </Grid>

                    <Grid item xs={12}>
                      <Typography variant="subtitle2" gutterBottom>
                        Response Body
                      </Typography>
                      <Box sx={{ bgcolor: '#f5f5f5', p: 2, borderRadius: 1 }}>
                        <pre>{transformedData.transformed.response_body || 'N/A'}</pre>
                      </Box>
                    </Grid>
                  </Grid>
                </TabPanel>
              )}
            </>
          )}
        </Paper>
      )}
    </div>
  );
};

export default WebhookTester;