import React from 'react';
import { Link } from 'react-router-dom';
import { Box, Typography, Button, Paper } from '@mui/material';
import HomeIcon from '@mui/icons-material/Home';

const NotFound: React.FC = () => {
  return (
    <Paper sx={{ p: 4, textAlign: 'center' }}>
      <Typography variant="h2" gutterBottom>
        404
      </Typography>
      <Typography variant="h5" gutterBottom>
        Page Not Found
      </Typography>
      <Typography variant="body1" color="textSecondary" paragraph>
        Sorry, we couldn't find the page you're looking for.
      </Typography>
      <Box mt={3}>
        <Button
          variant="contained"
          color="primary"
          component={Link}
          to="/"
          startIcon={<HomeIcon />}
        >
          Back to Dashboard
        </Button>
      </Box>
    </Paper>
  );
};

export default NotFound;