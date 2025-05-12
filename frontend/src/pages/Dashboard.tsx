import React from 'react';
import { useQuery } from 'react-query';
import { useNavigate } from 'react-router-dom';
import {
  Box,
  Card,
  CardContent,
  Grid,
  Typography,
  CircularProgress,
  Button,
  Paper,
  List,
  ListItem,
  ListItemText,
  Divider,
} from '@mui/material';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import { webhookApi, eventApi } from '../api/apiClient';
import PageHeader from '../components/PageHeader';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];
const STATUS_COLORS = {
  success: '#4caf50',
  error: '#f44336',
};

const Dashboard: React.FC = () => {
  const navigate = useNavigate();
  
  const { data: webhooks, isLoading: isLoadingWebhooks } = useQuery(
    'webhooks', 
    webhookApi.getAll
  );
  
  const { data: stats, isLoading: isLoadingStats } = useQuery(
    'eventStats', 
    eventApi.getStats
  );
  
  const { data: recentEvents, isLoading: isLoadingEvents } = useQuery(
    'recentEvents', 
    () => eventApi.getRecentEvents(5)
  );

  // Generate dummy data for demonstration
  const generateWebhookStats = () => {
    if (!webhooks || !Array.isArray(webhooks)) return [];

    return webhooks.map(webhook => ({
      name: webhook.source_path,
      received: Math.floor(Math.random() * 100),
      delivered: Math.floor(Math.random() * 80),
    }));
  };
  
  const generateSuccessRate = () => {
    if (!stats) {
      return [
        { name: 'Success', value: 85 },
        { name: 'Failed', value: 15 },
      ];
    }
    
    return [
      { name: 'Success', value: stats.success_rate * 100 },
      { name: 'Failed', value: (1 - stats.success_rate) * 100 },
    ];
  };

  if (isLoadingWebhooks || isLoadingStats || isLoadingEvents) {
    return (
      <Box display="flex" justifyContent="center" p={4}>
        <CircularProgress />
      </Box>
    );
  }

  const webhookCount = webhooks?.length || 0;
  const eventCount = stats?.received || 0;
  const successRate = stats?.success_rate || 0;
  
  const webhookStats = generateWebhookStats();
  const successRateData = generateSuccessRate();

  return (
    <div>
      <PageHeader title="Dashboard" />

      <Grid container spacing={3}>
        {/* Stats Cards */}
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total Webhooks
              </Typography>
              <Typography variant="h3">
                {webhookCount}
              </Typography>
              <Box mt={2}>
                <Button 
                  variant="outlined" 
                  size="small"
                  onClick={() => navigate('/webhooks')}
                >
                  View All
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total Events
              </Typography>
              <Typography variant="h3">
                {eventCount}
              </Typography>
              <Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
                Last 24 hours
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Success Rate
              </Typography>
              <Typography variant="h3">
                {(successRate * 100).toFixed(1)}%
              </Typography>
              <Typography variant="body2" color="textSecondary" sx={{ mt: 1 }}>
                Events successfully delivered
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        {/* Charts */}
        <Grid item xs={12} md={8}>
          <Paper sx={{ p: 3, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Webhook Activity
            </Typography>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart
                data={webhookStats}
                margin={{
                  top: 5,
                  right: 30,
                  left: 20,
                  bottom: 5,
                }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Bar dataKey="received" fill="#8884d8" name="Events Received" />
                <Bar dataKey="delivered" fill="#82ca9d" name="Events Delivered" />
              </BarChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>
        
        <Grid item xs={12} md={4}>
          <Paper sx={{ p: 3, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Delivery Success Rate
            </Typography>
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={successRateData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                  outerRadius={80}
                  fill="#8884d8"
                  dataKey="value"
                >
                  {successRateData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </Paper>
        </Grid>

        {/* Recent Events */}
        <Grid item xs={12}>
          <Paper>
            <Box p={3}>
              <Typography variant="h6" gutterBottom>
                Recent Events
              </Typography>
              
              <List>
                {recentEvents && recentEvents.length > 0 ? (
                  recentEvents.map((event, index) => (
                    <React.Fragment key={event.id}>
                      <ListItem>
                        <ListItemText
                          primary={
                            <Box display="flex" justifyContent="space-between">
                              <Typography variant="body1">
                                {event.source_path}
                              </Typography>
                              <Typography 
                                variant="body2" 
                                sx={{ 
                                  color: event.success ? STATUS_COLORS.success : STATUS_COLORS.error 
                                }}
                              >
                                {event.success ? 'Success' : 'Failed'}
                              </Typography>
                            </Box>
                          }
                          secondary={
                            <Box display="flex" justifyContent="space-between">
                              <Typography variant="body2" color="textSecondary">
                                ID: {event.id.substring(0, 8)}...
                              </Typography>
                              <Typography variant="body2" color="textSecondary">
                                {new Date(event.timestamp).toLocaleString()}
                              </Typography>
                            </Box>
                          }
                        />
                      </ListItem>
                      {index < recentEvents.length - 1 && <Divider />}
                    </React.Fragment>
                  ))
                ) : (
                  <ListItem>
                    <ListItemText
                      primary="No recent events found"
                      secondary="Webhook events will appear here when received"
                    />
                  </ListItem>
                )}
              </List>
              
              <Box mt={2} display="flex" justifyContent="flex-end">
                <Button 
                  variant="outlined" 
                  size="small"
                  onClick={() => navigate('/query')}
                >
                  Run Custom Query
                </Button>
              </Box>
            </Box>
          </Paper>
        </Grid>
      </Grid>
    </div>
  );
};

export default Dashboard;