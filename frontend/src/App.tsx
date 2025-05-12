import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Container from '@mui/material/Container';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import WebhookList from './pages/WebhookList';
import WebhookForm from './pages/WebhookForm';
import WebhookDetail from './pages/WebhookDetail';
import ReferenceTableList from './pages/ReferenceTableList';
import ReferenceTableUpload from './pages/ReferenceTableUpload';
import UdfList from './pages/UdfList';
import UdfForm from './pages/UdfForm';
import WebhookTester from './pages/WebhookTester';
import SqlQuery from './pages/SqlQuery';
import NotFound from './pages/NotFound';

function App() {
  return (
    <Layout>
      <Container maxWidth="lg" className="content-wrapper">
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/webhooks" element={<WebhookList />} />
          <Route path="/webhooks/new" element={<WebhookForm />} />
          <Route path="/webhooks/:id" element={<WebhookDetail />} />
          <Route path="/webhooks/:id/edit" element={<WebhookForm />} />
          <Route path="/reference-tables" element={<ReferenceTableList />} />
          <Route path="/reference-tables/upload" element={<ReferenceTableUpload />} />
          <Route path="/udfs" element={<UdfList />} />
          <Route path="/udfs/new" element={<UdfForm />} />
          <Route path="/udfs/:id/edit" element={<UdfForm />} />
          <Route path="/tester" element={<WebhookTester />} />
          <Route path="/query" element={<SqlQuery />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </Container>
    </Layout>
  );
}

export default App;