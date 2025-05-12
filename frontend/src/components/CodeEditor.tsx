import React from 'react';
import { Box } from '@mui/material';
import Editor from '@monaco-editor/react';

interface CodeEditorProps {
  value: string;
  onChange: (value: string | undefined) => void;
  language: 'sql' | 'python' | 'json';
  height?: string;
  readOnly?: boolean;
}

const CodeEditor: React.FC<CodeEditorProps> = ({
  value,
  onChange,
  language,
  height = '300px',
  readOnly = false,
}) => {
  return (
    <Box className="code-editor" sx={{ height }}>
      <Editor
        height="100%"
        language={language}
        value={value}
        onChange={onChange}
        options={{
          minimap: { enabled: false },
          fontSize: 14,
          scrollBeyondLastLine: false,
          wordWrap: 'on',
          readOnly,
          automaticLayout: true,
        }}
      />
    </Box>
  );
};

export default CodeEditor;