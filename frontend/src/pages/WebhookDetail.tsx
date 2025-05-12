import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Divider,
  Typography,
  Grid,
  CircularProgress,
  Paper,
  Chip,
  Button,
  Tabs,
  Tab,
  Switch,
  FormControlLabel,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import { webhookApi, referenceTableApi, udfApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import CodeEditor from '../components/CodeEditor';
import DataTable from '../components/DataTable';
import { useSnackbar } from 'notistack';

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
        <Box sx={{ pt: 3 }}>
          {children}
        </Box>
      )}
    </div>
  );
}

const WebhookDetail: React.FC = () => {
  const { id = '' } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { enqueueSnackbar } = useSnackbar();
  const [tabValue, setTabValue] = React.useState(0);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const { 
    data: webhook, 
    isLoading: isLoadingWebhook, 
    error: webhookError,
    refetch: refetchWebhook
  } = useQuery(['webhook', id], () => webhookApi.getById(id));

  const { 
    data: referenceTables, 
    isLoading: isLoadingTables,
    refetch: refetchTables
  } = useQuery(
    ['referenceTables', id], 
    () => referenceTableApi.getByWebhookId(id),
    { enabled: !!id }
  );

  const { 
    data: udfs, 
    isLoading: isLoadingUdfs,
    refetch: refetchUdfs
  } = useQuery(
    ['udfs', id], 
    () => udfApi.getByWebhookId(id),
    { enabled: !!id }
  );

  const handleToggleStatus = async () => {
    if (!webhook) return;
    
    try {
      await webhookApi.toggleStatus(id, !webhook.active);
      enqueueSnackbar(`Webhook ${webhook.active ? 'deactivated' : 'activated'} successfully`, { variant: 'success' });
      refetchWebhook();
    } catch (error) {
      enqueueSnackbar(`Failed to update webhook status: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    }
  };

  const handleDeleteReferenceTable = async (tableId: string) => {
    if (window.confirm('Are you sure you want to delete this reference table?')) {
      try {
        await referenceTableApi.delete(tableId);
        enqueueSnackbar('Reference table deleted successfully', { variant: 'success' });
        refetchTables();
      } catch (error) {
        enqueueSnackbar(`Failed to delete reference table: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
      }
    }
  };

  const handleDeleteUdf = async (udfId: string) => {
    if (window.confirm('Are you sure you want to delete this UDF?')) {
      try {
        await udfApi.delete(udfId);
        enqueueSnackbar('UDF deleted successfully', { variant: 'success' });
        refetchUdfs();
      } catch (error) {
        enqueueSnackbar(`Failed to delete UDF: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
      }
    }
  };

  const tableColumns = [
    { id: 'name', label: 'Table Name', minWidth: 120 },
    { id: 'description', label: 'Description', minWidth: 200 },
    { id: 'created_at', label: 'Created At', minWidth: 120, 
      format: (value: string) => new Date(value).toLocaleString() 
    },
    { id: 'actions', label: 'Actions', minWidth: 100, align: 'right' as const,
      format: (value: string) => (
        <Button
          variant="outlined"
          color="error"
          size="small"
          onClick={() => handleDeleteReferenceTable(value)}
        >
          Delete
        </Button>
      )
    }
  ];

  const udfColumns = [
    { id: 'name', label: 'UDF Name', minWidth: 120 },
    { id: 'created_at', label: 'Created At', minWidth: 120,
      format: (value: string) => new Date(value).toLocaleString()
    },
    { id: 'actions', label: 'Actions', minWidth: 100, align: 'right' as const,
      format: (value: string) => (
        <Button
          variant="outlined"
          color="error"
          size="small"
          onClick={() => handleDeleteUdf(value)}
        >
          Delete
        </Button>
      )
    }
  ];

  const formattedTableRows = referenceTables?.map(table => ({
    name: table.name,
    description: table.description,
    created_at: table.created_at,
    actions: table.id
  })) || [];

  const formattedUdfRows = udfs?.map(udf => ({
    name: udf.name,
    created_at: udf.created_at,
    actions: udf.id
  })) || [];

  if (isLoadingWebhook) {
    return (
      <Box display="flex" justifyContent="center" p={4}>
        <CircularProgress />
      </Box>
    );
  }

  if (webhookError || !webhook) {
    return (
      <Paper sx={{ p: 3, bgcolor: '#ffebee' }}>
        <Typography color="error" variant="body1">
          Error loading webhook: {webhookError instanceof Error ? webhookError.message : 'Unknown error'}
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
        title={`Webhook: ${webhook.source_path}`}
        buttonText="Edit"
        buttonPath={`/webhooks/${id}/edit`}
        secondaryButtonText="Back"
        secondaryButtonPath="/webhooks"
      />

      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h5" component="h2" sx={{ display: 'flex', alignItems: 'center' }}>
              {webhook.source_path}
              <Chip 
                label={webhook.active ? 'Active' : 'Inactive'} 
                color={webhook.active ? 'success' : 'default'}
                size="small" 
                sx={{ ml: 2 }}
              />
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={webhook.active}
                  onChange={handleToggleStatus}
                  color="primary"
                />
              }
              label={webhook.active ? 'Active' : 'Inactive'}
            />
          </Box>

          <Grid container spacing={3}>
            <Grid item xs={12} md={6}>
              <Typography variant="body2" color="textSecondary">
                Destination URL
              </Typography>
              <Typography variant="body1" gutterBottom>
                {webhook.destination_url}
              </Typography>
            </Grid>
            
            <Grid item xs={12} md={3}>
              <Typography variant="body2" color="textSecondary">
                Owner
              </Typography>
              <Typography variant="body1" gutterBottom>
                {webhook.owner}
              </Typography>
            </Grid>
            
            <Grid item xs={12} md={3}>
              <Typography variant="body2" color="textSecondary">
                Created At
              </Typography>
              <Typography variant="body1" gutterBottom>
                {new Date(webhook.created_at).toLocaleString()}
              </Typography>
            </Grid>
          </Grid>

          <Divider sx={{ my: 3 }} />

          <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
            <Tabs value={tabValue} onChange={handleTabChange}>
              <Tab label="Transform & Filter" id="webhook-tab-0" />
              <Tab label="Reference Tables" id="webhook-tab-1" />
              <Tab label="User-Defined Functions" id="webhook-tab-2" />
            </Tabs>
          </Box>

          <TabPanel value={tabValue} index={0}>
            <Typography variant="h6" gutterBottom>
              Transform Query
            </Typography>
            <CodeEditor
              language="sql"
              value={webhook.transform_query}
              onChange={() => {}}
              readOnly
            />

            {webhook.filter_query && (
              <>
                <Typography variant="h6" gutterBottom sx={{ mt: 3 }}>
                  Filter Query
                </Typography>
                <CodeEditor
                  language="sql"
                  value={webhook.filter_query}
                  onChange={() => {}}
                  readOnly
                />
              </>
            )}
          </TabPanel>

          <TabPanel value={tabValue} index={1}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6">Reference Tables</Typography>
              <Button 
                variant="contained" 
                color="primary"
                onClick={() => navigate('/reference-tables/upload', { state: { webhookId: id } })}
              >
                Upload New Table
              </Button>
            </Box>
            <DataTable 
              columns={tableColumns}
              rows={formattedTableRows}
              loading={isLoadingTables}
            />
          </TabPanel>

          <TabPanel value={tabValue} index={2}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6">User-Defined Functions</Typography>
              <Button 
                variant="contained" 
                color="primary"
                onClick={() => navigate('/udfs/new', { state: { webhookId: id } })}
              >
                Create New UDF
              </Button>
            </Box>
            <DataTable 
              columns={udfColumns}
              rows={formattedUdfRows}
              loading={isLoadingUdfs}
            />
            
            {udfs && udfs.length > 0 && (
              <Box sx={{ mt: 4 }}>
                <Typography variant="h6" gutterBottom>
                  UDF Code Example
                </Typography>
                <Typography variant="body2" color="textSecondary" paragraph>
                  Selected UDF code:
                </Typography>
                <CodeEditor
                  language="python"
                  value={udfs[0].code}
                  onChange={() => {}}
                  readOnly
                />
              </Box>
            )}
          </TabPanel>
        </CardContent>
      </Card>
    </div>
  );
};

export default WebhookDetail;