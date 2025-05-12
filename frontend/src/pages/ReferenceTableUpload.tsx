import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useQuery } from 'react-query';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Grid,
  TextField,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  CircularProgress,
  Paper,
  Alert,
  FormHelperText,
  SelectChangeEvent,
} from '@mui/material';
import { useSnackbar } from 'notistack';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import { webhookApi, referenceTableApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';

const ReferenceTableUpload: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { enqueueSnackbar } = useSnackbar();
  
  const [webhookId, setWebhookId] = useState<string>(
    location.state?.webhookId || ''
  );
  const [tableName, setTableName] = useState<string>('');
  const [description, setDescription] = useState<string>('');
  const [file, setFile] = useState<File | null>(null);
  const [filePreview, setFilePreview] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  
  const { data: webhooks, isLoading: isLoadingWebhooks } = useQuery(
    'webhooks',
    webhookApi.getAll
  );
  
  const fileInputRef = React.useRef<HTMLInputElement>(null);
  
  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.files && event.target.files[0]) {
      const selectedFile = event.target.files[0];
      setFile(selectedFile);
      
      // Read file for preview
      const reader = new FileReader();
      reader.onload = (e) => {
        if (e.target?.result) {
          const content = e.target.result as string;
          // Only show the first few lines
          const lines = content.split('\n').slice(0, 10).join('\n');
          setFilePreview(lines);
        }
      };
      reader.readAsText(selectedFile);
      
      // Try to extract a default table name from the file name
      if (!tableName) {
        const fileName = selectedFile.name;
        const nameWithoutExtension = fileName.split('.')[0];
        setTableName(nameWithoutExtension);
      }
    }
  };
  
  const validateForm = () => {
    if (!webhookId) {
      enqueueSnackbar('Please select a webhook', { variant: 'error' });
      return false;
    }
    
    if (!tableName) {
      enqueueSnackbar('Please enter a table name', { variant: 'error' });
      return false;
    }
    
    if (!file) {
      enqueueSnackbar('Please select a file to upload', { variant: 'error' });
      return false;
    }
    
    return true;
  };
  
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!validateForm()) return;
    
    setLoading(true);
    
    try {
      const formData = new FormData();
      formData.append('webhook_id', webhookId);
      formData.append('table_name', tableName);
      formData.append('description', description);
      if (file) {
        formData.append('file', file);
      }
      
      await referenceTableApi.upload(formData);
      
      enqueueSnackbar('Reference table uploaded successfully', { variant: 'success' });
      
      // Redirect to reference tables list or webhook detail
      if (location.state?.webhookId) {
        navigate(`/webhooks/${webhookId}`);
      } else {
        navigate('/reference-tables');
      }
    } catch (error) {
      enqueueSnackbar(`Failed to upload reference table: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };
  
  const triggerFileInput = () => {
    fileInputRef.current?.click();
  };
  
  return (
    <div>
      <PageHeader
        title="Upload Reference Table"
        buttonText="Cancel"
        buttonPath={location.state?.webhookId ? `/webhooks/${webhookId}` : '/reference-tables'}
      />
      
      <Card>
        <CardContent>
          <Box component="form" onSubmit={handleSubmit} noValidate>
            <Grid container spacing={3}>
              <Grid item xs={12}>
                <FormControl fullWidth required>
                  <InputLabel id="webhook-select-label">Webhook</InputLabel>
                  <Select
                    labelId="webhook-select-label"
                    id="webhook-select"
                    value={webhookId}
                    label="Webhook *"
                    onChange={(e: SelectChangeEvent) => setWebhookId(e.target.value)}
                    disabled={isLoadingWebhooks || !!location.state?.webhookId}
                  >
                    {webhooks?.map((webhook) => (
                      <MenuItem key={webhook.id} value={webhook.id}>
                        {webhook.source_path} ({webhook.owner})
                      </MenuItem>
                    ))}
                  </Select>
                  <FormHelperText>
                    Select the webhook that will use this reference table
                  </FormHelperText>
                </FormControl>
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  required
                  fullWidth
                  id="table-name"
                  label="Table Name"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  helperText="Name to identify this table (e.g., users, products)"
                />
              </Grid>
              
              <Grid item xs={12} md={6}>
                <TextField
                  fullWidth
                  id="description"
                  label="Description"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  helperText="Optional description of this table's purpose"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Typography variant="subtitle1" gutterBottom>
                  Upload CSV File
                </Typography>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  style={{ display: 'none' }}
                  onChange={handleFileChange}
                />
                <Box
                  sx={{
                    border: '2px dashed #ccc',
                    borderRadius: 2,
                    p: 3,
                    textAlign: 'center',
                    cursor: 'pointer',
                    '&:hover': {
                      borderColor: 'primary.main',
                      backgroundColor: 'rgba(0, 0, 0, 0.02)',
                    },
                  }}
                  onClick={triggerFileInput}
                >
                  {file ? (
                    <>
                      <Typography variant="body1" gutterBottom>
                        Selected file: <strong>{file.name}</strong> ({(file.size / 1024).toFixed(2)} KB)
                      </Typography>
                      <Button
                        variant="outlined"
                        size="small"
                        startIcon={<UploadFileIcon />}
                      >
                        Change File
                      </Button>
                    </>
                  ) : (
                    <>
                      <UploadFileIcon fontSize="large" color="action" />
                      <Typography variant="body1">
                        Click to select a CSV file or drag and drop it here
                      </Typography>
                    </>
                  )}
                </Box>
              </Grid>
              
              {filePreview && (
                <Grid item xs={12}>
                  <Typography variant="subtitle1" gutterBottom>
                    File Preview
                  </Typography>
                  <Paper sx={{ p: 2, maxHeight: 200, overflow: 'auto' }}>
                    <pre style={{ margin: 0 }}>{filePreview}</pre>
                  </Paper>
                  <Typography variant="caption" color="textSecondary">
                    Showing first 10 lines of the file
                  </Typography>
                </Grid>
              )}
              
              <Grid item xs={12}>
                <Alert severity="info">
                  <Typography variant="body2">
                    The CSV file should have a header row with column names. In SQL queries,
                    you can access this table using the name: <code>{`ref_${webhookId ? '_' + webhookId.replace(/-/g, '_') : '<webhook_id>'}_{tableName || '<table_name>'}`}</code>
                  </Typography>
                </Alert>
              </Grid>
              
              <Grid item xs={12}>
                <Box display="flex" justifyContent="flex-end">
                  <Button
                    variant="contained"
                    color="primary"
                    type="submit"
                    disabled={loading}
                    startIcon={loading ? <CircularProgress size={20} /> : <UploadFileIcon />}
                  >
                    {loading ? 'Uploading...' : 'Upload Table'}
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

export default ReferenceTableUpload;