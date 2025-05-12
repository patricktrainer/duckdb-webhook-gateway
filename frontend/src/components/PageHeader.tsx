import React from 'react';
import { Typography, Button, Box } from '@mui/material';
import { useNavigate } from 'react-router-dom';

interface PageHeaderProps {
  title: string;
  buttonText?: string;
  buttonPath?: string;
  buttonAction?: () => void;
  secondaryButtonText?: string;
  secondaryButtonPath?: string;
  secondaryButtonAction?: () => void;
}

const PageHeader: React.FC<PageHeaderProps> = ({
  title,
  buttonText,
  buttonPath,
  buttonAction,
  secondaryButtonText,
  secondaryButtonPath,
  secondaryButtonAction,
}) => {
  const navigate = useNavigate();

  const handleButtonClick = () => {
    if (buttonAction) {
      buttonAction();
    } else if (buttonPath) {
      navigate(buttonPath);
    }
  };

  const handleSecondaryButtonClick = () => {
    if (secondaryButtonAction) {
      secondaryButtonAction();
    } else if (secondaryButtonPath) {
      navigate(secondaryButtonPath);
    }
  };

  return (
    <Box className="header-with-button" mb={3}>
      <Typography variant="h4" component="h1">
        {title}
      </Typography>
      <Box>
        {secondaryButtonText && (
          <Button
            variant="outlined"
            color="primary"
            onClick={handleSecondaryButtonClick}
            sx={{ marginRight: 2 }}
          >
            {secondaryButtonText}
          </Button>
        )}
        {buttonText && (
          <Button 
            variant="contained" 
            color="primary"
            onClick={handleButtonClick}
          >
            {buttonText}
          </Button>
        )}
      </Box>
    </Box>
  );
};

export default PageHeader;