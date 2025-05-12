import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQuery } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  CircularProgress,
  Button,
  Paper,
  Divider,
  IconButton,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  TextField,
} from '@mui/material';
import VisibilityIcon from '@mui/icons-material/Visibility';
import DeleteIcon from '@mui/icons-material/Delete';
import { referenceTableApi, webhookApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import DataTable from '../components/DataTable';
import { useSnackbar } from 'notistack';

const ReferenceTableList: React.FC = () => {
  const navigate = useNavigate();
  const { enqueueSnackbar } = useSnackbar();
  const [selectedTable, setSelectedTable] = useState<any>(null);
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewData, setPreviewData] = useState<any>(null);
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false);

  const { data: tables, isLoading, error, refetch } = useQuery(
    'referenceTables',
    referenceTableApi.getAll
  );

  const { data: webhooks } = useQuery('webhooks', webhookApi.getAll);

  const getWebhookName = (webhookId: string) => {
    const webhook = webhooks?.find(w => w.id === webhookId);
    return webhook ? webhook.source_path : 'Unknown';
  };

  const handleOpenPreview = (table: any) => {
    setSelectedTable(table);
    // In a real implementation, we would fetch the actual table data here
    // For this demo, we'll just generate mock data
    const mockData = {
      columns: ['id', 'name', 'value', 'updated_at'],
      rows: Array(5).fill(0).map((_, i) => [
        i + 1,
        `record_${i + 1}`,
        Math.floor(Math.random() * 1000),
        new Date().toISOString(),
      ]),
    };
    setPreviewData(mockData);
    setPreviewOpen(true);
  };

  const handleConfirmDelete = (table: any) => {
    setSelectedTable(table);
    setConfirmDeleteOpen(true);
  };

  const handleDelete = async () => {
    if (!selectedTable) return;
    
    try {
      await referenceTableApi.delete(selectedTable.id);
      enqueueSnackbar('Reference table deleted successfully', { variant: 'success' });
      setConfirmDeleteOpen(false);
      refetch();
    } catch (error) {
      enqueueSnackbar(`Failed to delete reference table: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    }
  };

  const tableColumns = [
    { id: 'name', label: 'Table Name', minWidth: 120 },
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
    { id: 'description', label: 'Description', minWidth: 200 },
    { id: 'created_at', label: 'Created At', minWidth: 120,
      format: (value: string) => new Date(value).toLocaleString()
    },
    { id: 'actions', label: 'Actions', minWidth: 140, align: 'right' as const,
      format: (value: any) => (
        <Box>
          <IconButton
            color="primary"
            size="small"
            onClick={() => handleOpenPreview(value)}
            title="Preview data"
          >
            <VisibilityIcon />
          </IconButton>
          <IconButton
            color="error"
            size="small"
            onClick={() => handleConfirmDelete(value)}
            title="Delete table"
          >
            <DeleteIcon />
          </IconButton>
        </Box>
      )
    }
  ];

  const formattedRows = tables?.map(table => ({
    name: table.name,
    webhook: table.webhook_id,
    description: table.description,
    created_at: table.created_at,
    actions: table
  })) || [];

  const previewColumns = previewData?.columns.map((col: string) => ({
    id: col,
    label: col,
    minWidth: 100
  })) || [];

  const previewRows = previewData?.rows.map((row: any[]) => {
    const rowObj: Record<string, any> = {};
    previewData.columns.forEach((col: string, index: number) => {
      rowObj[col] = row[index];
    });
    return rowObj;
  }) || [];

  return (
    <div>
      <PageHeader
        title="Reference Tables"
        buttonText="Upload New Table"
        buttonPath="/reference-tables/upload"
      />

      {isLoading ? (
        <Box display="flex" justifyContent="center" p={4}>
          <CircularProgress />
        </Box>
      ) : error ? (
        <Typography color="error">
          Error loading reference tables: {error instanceof Error ? error.message : 'Unknown error'}
        </Typography>
      ) : tables && tables.length > 0 ? (
        <Paper>
          <DataTable
            columns={tableColumns}
            rows={formattedRows}
          />
        </Paper>
      ) : (
        <Card>
          <CardContent>
            <Typography align="center">
              No reference tables found. Click "Upload New Table" to create one.
            </Typography>
          </CardContent>
        </Card>
      )}

      {/* Preview Dialog */}
      <Dialog
        open={previewOpen}
        onClose={() => setPreviewOpen(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Table Preview: {selectedTable?.name}
        </DialogTitle>
        <DialogContent>
          <DialogContentText paragraph>
            Showing sample data from the reference table.
          </DialogContentText>
          <DataTable
            columns={previewColumns}
            rows={previewRows}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPreviewOpen(false)}>Close</Button>
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
            Are you sure you want to delete the reference table "{selectedTable?.name}"?
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

export default ReferenceTableList;