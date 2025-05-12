import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useQuery } from 'react-query';
import {
  Box,
  TextField,
  Button,
  Card,
  CardContent,
  Typography,
  Grid,
  CircularProgress,
  Paper,
} from '@mui/material';
import { useSnackbar } from 'notistack';
import { webhookApi, WebhookFormData } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import CodeEditor from '../components/CodeEditor';

const WebhookForm: React.FC = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { enqueueSnackbar } = useSnackbar();
  const isEditMode = !!id;

  const [formValues, setFormValues] = useState<WebhookFormData>({
    source_path: '',
    destination_url: '',
    transform_query: '',
    filter_query: '',
    owner: '',
  });

  const [loading, setLoading] = useState(false);

  const { data: webhook, isLoading, error } = useQuery(
    ['webhook', id],
    () => webhookApi.getById(id!),
    {
      enabled: isEditMode,
    }
  );

  useEffect(() => {
    if (webhook) {
      setFormValues({
        source_path: webhook.source_path,
        destination_url: webhook.destination_url,
        transform_query: webhook.transform_query,
        filter_query: webhook.filter_query || '',
        owner: webhook.owner,
      });
    }
  }, [webhook]);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormValues((prev) => ({
      ...prev,
      [name]: value,
    }));
  };

  const handleTransformQueryChange = (value: string | undefined) => {
    setFormValues((prev) => ({
      ...prev,
      transform_query: value || '',
    }));
  };

  const handleFilterQueryChange = (value: string | undefined) => {
    setFormValues((prev) => ({
      ...prev,
      filter_query: value || '',
    }));
  };

  const validateForm = (): boolean => {
    if (!formValues.source_path) {
      enqueueSnackbar('Source path is required', { variant: 'error' });
      return false;
    }
    if (!formValues.destination_url) {
      enqueueSnackbar('Destination URL is required', { variant: 'error' });
      return false;
    }
    if (!formValues.transform_query) {
      enqueueSnackbar('Transform query is required', { variant: 'error' });
      return false;
    }
    if (!formValues.owner) {
      enqueueSnackbar('Owner is required', { variant: 'error' });
      return false;
    }
    return true;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!validateForm()) return;
    
    setLoading(true);
    
    try {
      if (isEditMode) {
        await webhookApi.update(id!, formValues);
        enqueueSnackbar('Webhook updated successfully', { variant: 'success' });
      } else {
        await webhookApi.create(formValues);
        enqueueSnackbar('Webhook created successfully', { variant: 'success' });
      }
      navigate('/webhooks');
    } catch (error) {
      enqueueSnackbar(`Failed to ${isEditMode ? 'update' : 'create'} webhook: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };

  if (isEditMode && isLoading) {
    return (
      <Box display="flex" justifyContent="center" p={4}>
        <CircularProgress />
      </Box>
    );
  }

  if (isEditMode && error) {
    return (
      <Paper sx={{ p: 3, bgcolor: '#ffebee' }}>
        <Typography color="error" variant="body1">
          Error loading webhook: {error instanceof Error ? error.message : 'Unknown error'}
        </Typography>
        <Button variant="contained" onClick={() => navigate('/webhooks')} sx={{ mt: 2 }}>
          Go Back
        </Button>
      </Paper>
    );
  }

  return (
    <div>
      <PageHeader
        title={isEditMode ? 'Edit Webhook' : 'Register New Webhook'}
        buttonText="Cancel"
        buttonPath="/webhooks"
      />

      <Card>
        <CardContent>
          <Box component="form" onSubmit={handleSubmit} noValidate>
            <Grid container spacing={3}>
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  id="source_path"
                  label="Source Path"
                  name="source_path"
                  value={formValues.source_path}
                  onChange={handleChange}
                  placeholder="/github-events"
                  helperText="The endpoint path to receive webhooks (e.g., /github-events)"
                  margin="normal"
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  id="destination_url"
                  label="Destination URL"
                  name="destination_url"
                  value={formValues.destination_url}
                  onChange={handleChange}
                  placeholder="https://example.com/webhook-handler"
                  helperText="Where to forward transformed events"
                  margin="normal"
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  id="owner"
                  label="Owner"
                  name="owner"
                  value={formValues.owner}
                  onChange={handleChange}
                  placeholder="team-name"
                  helperText="Team or individual responsible for this webhook"
                  margin="normal"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Typography variant="h6" gutterBottom>
                  Transform Query
                </Typography>
                <Typography variant="body2" color="textSecondary" paragraph>
                  SQL query to transform incoming webhook data. Use {'{'}{'{'} payload {'}'}{'}'}  as a placeholder for the webhook payload.
                </Typography>
                <CodeEditor
                  language="sql"
                  value={formValues.transform_query}
                  onChange={handleTransformQueryChange}
                  height="200px"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Typography variant="h6" gutterBottom>
                  Filter Query (Optional)
                </Typography>
                <Typography variant="body2" color="textSecondary" paragraph>
                  SQL WHERE clause to filter which events get forwarded.
                </Typography>
                <CodeEditor
                  language="sql"
                  value={formValues.filter_query || ''}
                  onChange={handleFilterQueryChange}
                  height="150px"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Box display="flex" justifyContent="flex-end">
                  <Button
                    variant="contained"
                    color="primary"
                    type="submit"
                    disabled={loading}
                  >
                    {loading ? <CircularProgress size={24} /> : isEditMode ? 'Update Webhook' : 'Create Webhook'}
                  </Button>
                </Box>
              </Grid>
            </Grid>
          </Box>
        </CardContent>
      </Card>
    </div>
  );
};

export default WebhookForm;