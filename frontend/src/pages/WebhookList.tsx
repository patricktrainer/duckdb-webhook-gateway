import React from 'react';
import { useQuery } from 'react-query';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  CardContent,
  Typography,
  Grid,
  IconButton,
  Chip,
  Box,
  CircularProgress,
  Switch,
  FormControlLabel,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { webhookApi, Webhook } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import { useSnackbar } from 'notistack';

const WebhookList: React.FC = () => {
  const navigate = useNavigate();
  const { enqueueSnackbar } = useSnackbar();
  const { data: webhooks, isLoading, error, refetch } = useQuery('webhooks', webhookApi.getAll);

  const handleToggleStatus = async (webhook: Webhook) => {
    try {
      await webhookApi.toggleStatus(webhook.id, !webhook.active);
      enqueueSnackbar(`Webhook ${webhook.active ? 'deactivated' : 'activated'} successfully`, { variant: 'success' });
      refetch();
    } catch (error) {
      enqueueSnackbar(`Failed to update webhook status: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    }
  };

  const handleDelete = async (id: string) => {
    if (window.confirm('Are you sure you want to delete this webhook?')) {
      try {
        await webhookApi.delete(id);
        enqueueSnackbar('Webhook deleted successfully', { variant: 'success' });
        refetch();
      } catch (error) {
        enqueueSnackbar(`Failed to delete webhook: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
      }
    }
  };

  return (
    <div>
      <PageHeader
        title="Webhooks"
        buttonText="Register New Webhook"
        buttonPath="/webhooks/new"
      />

      {isLoading ? (
        <Box display="flex" justifyContent="center" p={4}>
          <CircularProgress />
        </Box>
      ) : error ? (
        <Typography color="error">
          Error loading webhooks: {error instanceof Error ? error.message : 'Unknown error'}
        </Typography>
      ) : webhooks && webhooks.length > 0 ? (
        <Grid container spacing={3}>
          {webhooks.map((webhook) => (
            <Grid item xs={12} key={webhook.id}>
              <Card>
                <CardContent>
                  <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
                    <Typography variant="h5" component="h2">
                      {webhook.source_path}
                    </Typography>
                    <Box>
                      <FormControlLabel
                        control={
                          <Switch
                            checked={webhook.active}
                            onChange={() => handleToggleStatus(webhook)}
                            color="primary"
                          />
                        }
                        label={webhook.active ? 'Active' : 'Inactive'}
                      />
                    </Box>
                  </Box>
                  
                  <Typography color="textSecondary" gutterBottom>
                    Destination: {webhook.destination_url}
                  </Typography>
                  
                  <Typography variant="body2" component="p" sx={{ mb: 1 }}>
                    Owner: <Chip label={webhook.owner} size="small" />
                  </Typography>
                  
                  <Typography variant="body2" component="p">
                    Created: {new Date(webhook.created_at).toLocaleString()}
                  </Typography>
                  
                  <Box display="flex" justifyContent="flex-end" mt={2}>
                    <IconButton 
                      color="primary" 
                      onClick={() => navigate(`/webhooks/${webhook.id}`)}
                      title="View details"
                    >
                      <VisibilityIcon />
                    </IconButton>
                    <IconButton 
                      color="primary" 
                      onClick={() => navigate(`/webhooks/${webhook.id}/edit`)}
                      title="Edit webhook"
                    >
                      <EditIcon />
                    </IconButton>
                    <IconButton 
                      color="error" 
                      onClick={() => handleDelete(webhook.id)}
                      title="Delete webhook"
                    >
                      <DeleteIcon />
                    </IconButton>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      ) : (
        <Card>
          <CardContent>
            <Typography align="center">
              No webhooks found. Click "Register New Webhook" to create one.
            </Typography>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

export default WebhookList;