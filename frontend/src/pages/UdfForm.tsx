import React, { useState } from 'react';
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
  Alert,
  FormHelperText,
  Paper,
  Divider,
  SelectChangeEvent,
} from '@mui/material';
import CodeIcon from '@mui/icons-material/Code';
import { useSnackbar } from 'notistack';
import { webhookApi, udfApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import CodeEditor from '../components/CodeEditor';

const DEFAULT_UDF_CODE = `def extract_value(text: str) -> str:
    """Extract a value from text"""
    if not text:
        return None
    
    # Add your custom logic here
    return text.strip()`;

const UdfForm: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { enqueueSnackbar } = useSnackbar();
  
  const [webhookId, setWebhookId] = useState<string>(
    location.state?.webhookId || ''
  );
  const [functionName, setFunctionName] = useState<string>('');
  const [functionCode, setFunctionCode] = useState<string>(DEFAULT_UDF_CODE);
  const [loading, setLoading] = useState<boolean>(false);
  
  const { data: webhooks, isLoading: isLoadingWebhooks } = useQuery(
    'webhooks',
    webhookApi.getAll
  );
  
  const handleFunctionCodeChange = (value: string | undefined) => {
    setFunctionCode(value || DEFAULT_UDF_CODE);
  };
  
  const validateForm = () => {
    if (!webhookId) {
      enqueueSnackbar('Please select a webhook', { variant: 'error' });
      return false;
    }
    
    if (!functionName) {
      enqueueSnackbar('Please enter a function name', { variant: 'error' });
      return false;
    }
    
    // Basic name validation
    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(functionName)) {
      enqueueSnackbar('Function name must start with a letter and contain only letters, numbers, and underscores', { variant: 'error' });
      return false;
    }
    
    if (!functionCode || functionCode.trim() === '') {
      enqueueSnackbar('Please enter the function code', { variant: 'error' });
      return false;
    }
    
    // Basic Python function validation
    if (!functionCode.includes('def ')) {
      enqueueSnackbar('Function code must contain a Python function definition (def)', { variant: 'error' });
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
      formData.append('function_name', functionName);
      formData.append('function_code', functionCode);
      
      await udfApi.register(formData);
      
      enqueueSnackbar('UDF registered successfully', { variant: 'success' });
      
      // Redirect to UDFs list or webhook detail
      if (location.state?.webhookId) {
        navigate(`/webhooks/${webhookId}`);
      } else {
        navigate('/udfs');
      }
    } catch (error) {
      enqueueSnackbar(`Failed to register UDF: ${error instanceof Error ? error.message : 'Unknown error'}`, { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };
  
  return (
    <div>
      <PageHeader
        title="Create User-Defined Function"
        buttonText="Cancel"
        buttonPath={location.state?.webhookId ? `/webhooks/${webhookId}` : '/udfs'}
      />
      
      <Card>
        <CardContent>
          <Typography variant="body1" paragraph>
            Create a custom Python function that can be used in your SQL queries to transform and process data.
          </Typography>
          
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
                    Select the webhook that will use this function
                  </FormHelperText>
                </FormControl>
              </Grid>
              
              <Grid item xs={12}>
                <TextField
                  required
                  fullWidth
                  id="function-name"
                  label="Function Name"
                  value={functionName}
                  onChange={(e) => setFunctionName(e.target.value)}
                  helperText="Name to identify this function (e.g., extract_email, format_date)"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Typography variant="subtitle1" gutterBottom>
                  Python Function Code
                </Typography>
                <Typography variant="body2" color="textSecondary" paragraph>
                  Define a Python function that takes at least one parameter and returns a value.
                </Typography>
                <CodeEditor
                  language="python"
                  value={functionCode}
                  onChange={handleFunctionCodeChange}
                  height="300px"
                />
              </Grid>
              
              <Grid item xs={12}>
                <Alert severity="info">
                  <Typography variant="subtitle2" gutterBottom>
                    UDF Usage Example
                  </Typography>
                  <Typography variant="body2">
                    Once registered, you can use this function in your SQL queries as:
                  </Typography>
                  <Box sx={{ mt: 1, p: 1, bgcolor: '#f5f5f5', borderRadius: 1 }}>
                    <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                      {`SELECT udf_${webhookId ? webhookId.substring(0, 8) + '...' : '<webhook_id>'}_{functionName || '<function_name>'}(column_name) FROM table`}
                    </Typography>
                  </Box>
                </Alert>
              </Grid>
              
              <Grid item xs={12}>
                <Paper sx={{ p: 3 }}>
                  <Typography variant="h6" gutterBottom>
                    UDF Guidelines
                  </Typography>
                  <Typography variant="body2" paragraph>
                    Follow these guidelines for reliable UDFs:
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <Typography variant="subtitle2" gutterBottom>
                        Function Signature
                      </Typography>
                      <ul>
                        <li>Must include type hints for parameters and return value</li>
                        <li>First parameter is the value to transform</li>
                        <li>Should handle None values gracefully</li>
                        <li>Include docstring explaining function purpose</li>
                      </ul>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Typography variant="subtitle2" gutterBottom>
                        Imports & Dependencies
                      </Typography>
                      <ul>
                        <li>Standard library imports are supported (re, json, datetime, etc.)</li>
                        <li>Avoid imports that may not be available in the DuckDB environment</li>
                        <li>Keep functions focused on data transformation, not I/O</li>
                        <li>Handle exceptions to prevent query failures</li>
                      </ul>
                    </Grid>
                  </Grid>
                </Paper>
              </Grid>
              
              <Grid item xs={12}>
                <Box display="flex" justifyContent="flex-end">
                  <Button
                    variant="contained"
                    color="primary"
                    type="submit"
                    disabled={loading}
                    startIcon={loading ? <CircularProgress size={20} /> : <CodeIcon />}
                  >
                    {loading ? 'Registering...' : 'Register UDF'}
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

export default UdfForm;