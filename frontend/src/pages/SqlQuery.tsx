import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Button,
  CircularProgress,
  Paper,
  Divider,
  Chip,
  Alert,
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import SaveIcon from '@mui/icons-material/Save';
import ClearIcon from '@mui/icons-material/Clear';
import { useSnackbar } from 'notistack';
import { queryApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';
import CodeEditor from '../components/CodeEditor';
import DataTable from '../components/DataTable';

const EXAMPLE_QUERIES = [
  {
    name: 'Recent Events',
    query: 'SELECT r.id, r.timestamp, r.source_path, r.payload, t.success, t.response_code \nFROM raw_events r \nLEFT JOIN transformed_events t ON r.id = t.raw_event_id \nORDER BY r.timestamp DESC \nLIMIT 10',
  },
  {
    name: 'Success Rate by Webhook',
    query: "SELECT w.source_path, COUNT(t.id) as total, SUM(CASE WHEN t.success THEN 1 ELSE 0 END) as success_count, \nCAST(SUM(CASE WHEN t.success THEN 1 ELSE 0 END) AS FLOAT) / COUNT(t.id) as success_rate \nFROM webhooks w \nJOIN transformed_events t ON w.id = t.webhook_id \nGROUP BY w.source_path",
  },
  {
    name: 'Failed Events',
    query: "SELECT r.id, r.timestamp, r.source_path, t.response_code, t.response_body \nFROM raw_events r \nJOIN transformed_events t ON r.id = t.raw_event_id \nWHERE t.success = FALSE \nORDER BY r.timestamp DESC \nLIMIT 10",
  },
  {
    name: 'Event Types',
    query: "SELECT json_extract(r.payload, '$.type') as event_type, COUNT(*) as count \nFROM raw_events r \nGROUP BY event_type \nORDER BY count DESC",
  },
];

const SqlQuery: React.FC = () => {
  const { enqueueSnackbar } = useSnackbar();
  const [query, setQuery] = useState<string>('SELECT * FROM webhooks LIMIT 10');
  const [loading, setLoading] = useState<boolean>(false);
  const [queryResult, setQueryResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [savedQueries, setSavedQueries] = useState<Array<{ name: string; query: string }>>([]);

  const handleQueryChange = (value: string | undefined) => {
    setQuery(value || '');
  };

  const handleRunQuery = async () => {
    if (!query.trim()) {
      enqueueSnackbar('Please enter a SQL query', { variant: 'error' });
      return;
    }

    setLoading(true);
    setQueryResult(null);
    setError(null);

    try {
      const result = await queryApi.executeQuery(query);
      setQueryResult(result);
      enqueueSnackbar('Query executed successfully', { variant: 'success' });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error occurred');
      enqueueSnackbar('Query execution failed', { variant: 'error' });
    } finally {
      setLoading(false);
    }
  };

  const handleSaveQuery = () => {
    const name = prompt('Enter a name for this query:');
    if (name && query.trim()) {
      setSavedQueries([...savedQueries, { name, query }]);
      enqueueSnackbar('Query saved', { variant: 'success' });
    }
  };

  const handleClearQuery = () => {
    setQuery('');
    setQueryResult(null);
    setError(null);
  };

  const handleLoadQuery = (selectedQuery: string) => {
    setQuery(selectedQuery);
    setQueryResult(null);
    setError(null);
  };

  const createTableColumns = () => {
    if (!queryResult || !queryResult.columns) return [];
    
    return queryResult.columns.map((col: string) => ({
      id: col,
      label: col,
      minWidth: 120,
      format: (value: any) => {
        if (value === null) return 'NULL';
        if (typeof value === 'object') return JSON.stringify(value);
        return String(value);
      }
    }));
  };

  const createTableRows = () => {
    if (!queryResult || !queryResult.rows) return [];
    
    return queryResult.rows.map((row: any[]) => {
      const rowObj: Record<string, any> = {};
      queryResult.columns.forEach((col: string, index: number) => {
        rowObj[col] = row[index];
      });
      return rowObj;
    });
  };

  return (
    <div>
      <PageHeader title="SQL Query" />

      <Card sx={{ mb: 4 }}>
        <CardContent>
          <Typography variant="h6" gutterBottom>
            Run SQL Queries
          </Typography>
          <Typography variant="body2" color="textSecondary" paragraph>
            Execute arbitrary SQL queries against the webhook gateway database.
          </Typography>

          <Box sx={{ mb: 3 }}>
            <Typography variant="subtitle2" gutterBottom>
              Example Queries
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
              {EXAMPLE_QUERIES.map((example, index) => (
                <Chip
                  key={index}
                  label={example.name}
                  onClick={() => handleLoadQuery(example.query)}
                  clickable
                />
              ))}
              {savedQueries.map((saved, index) => (
                <Chip
                  key={`saved-${index}`}
                  label={saved.name}
                  onClick={() => handleLoadQuery(saved.query)}
                  variant="outlined"
                  clickable
                />
              ))}
            </Box>
          </Box>

          <Box sx={{ mb: 2 }}>
            <CodeEditor
              language="sql"
              value={query}
              onChange={handleQueryChange}
            />
          </Box>

          <Box display="flex" justifyContent="flex-end" gap={2}>
            <Button
              variant="outlined"
              color="inherit"
              startIcon={<ClearIcon />}
              onClick={handleClearQuery}
            >
              Clear
            </Button>
            <Button
              variant="outlined"
              color="primary"
              startIcon={<SaveIcon />}
              onClick={handleSaveQuery}
              disabled={!query.trim()}
            >
              Save Query
            </Button>
            <Button
              variant="contained"
              color="primary"
              startIcon={loading ? <CircularProgress size={20} /> : <PlayArrowIcon />}
              onClick={handleRunQuery}
              disabled={loading || !query.trim()}
            >
              Run Query
            </Button>
          </Box>
        </CardContent>
      </Card>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {queryResult && (
        <Paper sx={{ p: 3 }}>
          <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
            <Typography variant="h6">
              Query Results
            </Typography>
            <Typography variant="body2" color="textSecondary">
              {queryResult.rows?.length || 0} rows returned
            </Typography>
          </Box>
          
          <Divider sx={{ mb: 3 }} />
          
          <DataTable
            columns={createTableColumns()}
            rows={createTableRows()}
          />
        </Paper>
      )}
    </div>
  );
};

export default SqlQuery;