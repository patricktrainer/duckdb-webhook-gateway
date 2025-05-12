import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  CircularProgress,
  Button,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  IconButton,
  Chip,
} from '@mui/material';
import VisibilityIcon from '@mui/icons-material/Visibility';
import DeleteIcon from '@mui/icons-material/Delete';
import { udfApi, webhookApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import DataTable from '../components/DataTable';
import CodeEditor from '../components/CodeEditor';
import { useSnackbar } from 'notistack';

const UdfList: React.FC = () => {
  const navigate = useNavigate();
  const { enqueueSnackbar } = useSnackbar();
  const [selectedUdf, setSelectedUdf] = useState<any>(null);
  const [codePreviewOpen, setCodePreviewOpen] = useState(false);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);

  const { data: udfs, isLoading, error, refetch } = useQuery(
    'udfs',
    udfApi.getAll
  );

  const { data: webhooks } = useQuery('webhooks', webhookApi.getAll);

  const getWebhookName = (webhookId: string) => {
    const webhook = webhooks?.find(w => w.id === webhookId);
    return webhook ? webhook.source_path : 'Unknown';
  };

  const handleOpenCodePreview = (udf: any) => {
    setSelectedUdf(udf);
    setCodePreviewOpen(true);
  };

  const handleConfirmDelete = (udf: any) => {
    setSelectedUdf(udf);
    setConfirmDeleteOpen(true);
  };

  const handleDelete = async () => {
    if (!selectedUdf) return;
    
    try {
      await udfApi.delete(selectedUdf.id);
      enqueueSnackbar('UDF deleted successfully', { variant: 'success' });
      setConfirmDeleteOpen(false);
      refetch();
    } catch (error) {
      enqueueSnackbar(`Failed to delete UDF: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    }
  };

  const columns = [
    { id: 'name', label: 'Function Name', minWidth: 150 },
    { id: 'webhook', label: 'Webhook', minWidth: 150,
      format: (value: string) => (
        <Chip 
          label={getWebhookName(value)}
          size="small"
          color="primary"
          variant="outlined"
          onClick={() => navigate(`/webhooks/${value}`)}
        />
      )
    },
    { id: 'created_at', label: 'Created At', minWidth: 120,
      format: (value: string) => new Date(value).toLocaleString()
    },
    { id: 'usage', label: 'Usage Example', minWidth: 200,
      format: (value: string) => (
        <Typography variant="body2" sx={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>
          udf_{value.split('_')[0]}_<strong>{value.split('_')[1]}</strong>(column_name)
        </Typography>
      )
    },
    { id: 'actions', label: 'Actions', minWidth: 140, align: 'right' as const,
      format: (value: any) => (
        <Box>
          <IconButton
            color="primary"
            size="small"
            onClick={() => handleOpenCodePreview(value)}
            title="View code"
          >
            <VisibilityIcon />
          </IconButton>
          <IconButton
            color="error"
            size="small"
            onClick={() => handleConfirmDelete(value)}
            title="Delete UDF"
          >
            <DeleteIcon />
          </IconButton>
        </Box>
      )
    }
  ];

  const formattedRows = udfs?.map(udf => ({
    name: udf.name,
    webhook: udf.webhook_id,
    created_at: udf.created_at,
    usage: `${udf.webhook_id}_${udf.name}`,
    actions: udf
  })) || [];

  return (
    <div>
      <PageHeader
        title="User-Defined Functions"
        buttonText="Create New UDF"
        buttonPath="/udfs/new"
      />

      {isLoading ? (
        <Box display="flex" justifyContent="center" p={4}>
          <CircularProgress />
        </Box>
      ) : error ? (
        <Typography color="error">
          Error loading UDFs: {error instanceof Error ? error.message : 'Unknown error'}
        </Typography>
      ) : udfs && udfs.length > 0 ? (
        <Paper>
          <DataTable
            columns={columns}
            rows={formattedRows}
          />
        </Paper>
      ) : (
        <Card>
          <CardContent>
            <Typography align="center">
              No User-Defined Functions found. Click "Create New UDF" to create one.
            </Typography>
          </CardContent>
        </Card>
      )}

      {/* Code Preview Dialog */}
      <Dialog
        open={codePreviewOpen}
        onClose={() => setCodePreviewOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          UDF: {selectedUdf?.name}
        </DialogTitle>
        <DialogContent>
          <DialogContentText paragraph>
            Function code:
          </DialogContentText>
          <CodeEditor
            language="python"
            value={selectedUdf?.code || ''}
            onChange={() => {}}
            readOnly
          />
          <DialogContentText sx={{ mt: 2 }}>
            <Typography variant="subtitle2">Usage in SQL:</Typography>
            <Box sx={{ p: 1, bgcolor: '#f5f5f5', borderRadius: 1, mt: 1 }}>
              <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                SELECT udf_{selectedUdf?.webhook_id}_{selectedUdf?.name}(column_name) FROM table
              </Typography>
            </Box>
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCodePreviewOpen(false)}>Close</Button>
        </DialogActions>
      </Dialog>

      {/* Confirm Delete Dialog */}
      <Dialog
        open={confirmDeleteOpen}
        onClose={() => setConfirmDeleteOpen(false)}
      >
        <DialogTitle>Confirm Deletion</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to delete the UDF "{selectedUdf?.name}"?
            This action cannot be undone.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setConfirmDeleteOpen(false)}>Cancel</Button>
          <Button onClick={handleDelete} color="error" autoFocus>
            Delete
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  );
};

export default UdfList;