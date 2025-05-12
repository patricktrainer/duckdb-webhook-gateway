import axios from 'axios';

// Create an axios instance with defaults
const apiClient = axios.create({
  baseURL: '/',
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add API key header to all requests
const apiKey = 'default_key'; // In a real app, this would be configurable
apiClient.interceptors.request.use(config => {
  config.headers['X-API-Key'] = apiKey;
  return config;
});

// Define API interface
export interface Webhook {
  id: string;
  source_path: string;
  destination_url: string;
  transform_query: string;
  filter_query?: string;
  owner: string;
  created_at: string;
  active: boolean;
}

export interface WebhookFormData {
  source_path: string;
  destination_url: string;
  transform_query: string;
  filter_query?: string;
  owner: string;
}

export interface ReferenceTable {
  id: string;
  webhook_id: string;
  name: string;
  description: string;
  created_at: string;
}

export interface UdfDefinition {
  id: string;
  webhook_id: string;
  name: string;
  code: string;
  created_at: string;
}

export interface EventStats {
  received: number;
  processed: number;
  success_rate: number;
  recent_events: Array<{
    id: string;
    timestamp: string;
    source_path: string;
    success: boolean;
  }>;
}

export interface SqlQueryResult {
  columns: string[];
  rows: any[];
}

// API functions
export const webhookApi = {
  getAll: async (): Promise<Webhook[]> => {
    const response = await apiClient.get('/webhooks');
    return response.data?.webhooks || [];
  },
  
  getById: async (id: string): Promise<Webhook> => {
    const response = await apiClient.get(`/webhook/${id}`);
    return response.data;
  },
  
  create: async (data: WebhookFormData): Promise<Webhook> => {
    const response = await apiClient.post('/register', data);
    return response.data;
  },
  
  update: async (id: string, data: Partial<WebhookFormData>): Promise<Webhook> => {
    const response = await apiClient.put(`/webhook/${id}`, data);
    return response.data;
  },
  
  delete: async (id: string): Promise<void> => {
    await apiClient.delete(`/webhook/${id}`);
  },
  
  toggleStatus: async (id: string, active: boolean): Promise<Webhook> => {
    const response = await apiClient.patch(`/webhook/${id}/status`, { active });
    return response.data;
  }
};

export const referenceTableApi = {
  getAll: async (): Promise<ReferenceTable[]> => {
    const response = await apiClient.get('/reference_tables');
    return response.data?.reference_tables || [];
  },
  
  getByWebhookId: async (webhookId: string): Promise<ReferenceTable[]> => {
    const response = await apiClient.get(`/reference_tables/${webhookId}`);
    return response.data;
  },
  
  upload: async (formData: FormData): Promise<ReferenceTable> => {
    const response = await apiClient.post('/upload_table', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },
  
  delete: async (tableId: string): Promise<void> => {
    await apiClient.delete(`/reference_table/${tableId}`);
  }
};

export const udfApi = {
  getAll: async (): Promise<UdfDefinition[]> => {
    const response = await apiClient.get('/udfs');
    return response.data?.udfs || [];
  },
  
  getByWebhookId: async (webhookId: string): Promise<UdfDefinition[]> => {
    const response = await apiClient.get(`/udfs/${webhookId}`);
    return response.data;
  },
  
  register: async (formData: FormData): Promise<UdfDefinition> => {
    const response = await apiClient.post('/register_udf', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },
  
  delete: async (udfId: string): Promise<void> => {
    await apiClient.delete(`/udf/${udfId}`);
  }
};

export const eventApi = {
  getStats: async (): Promise<EventStats> => {
    const response = await apiClient.get('/stats');
    return response.data && response.data.status === 'success' ? response.data : {
      received: 0,
      processed: 0,
      success_rate: 0,
      recent_events: []
    };
  },

  getRecentEvents: async (limit: number = 10): Promise<any[]> => {
    const response = await apiClient.get(`/events?limit=${limit}`);
    return response.data?.events || [];
  }
};

export const queryApi = {
  executeQuery: async (query: string): Promise<SqlQueryResult> => {
    const formData = new FormData();
    formData.append('query', query);
    
    const response = await apiClient.post('/query', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  }
};

export const testApi = {
  sendWebhook: async (path: string, payload: any): Promise<any> => {
    const response = await axios.post(path, payload, {
      headers: {
        'Content-Type': 'application/json',
      },
    });
    return response.data;
  },

  getTransformedEvent: async (eventId: string): Promise<any> => {
    const response = await apiClient.get(`/event/${eventId}/transformed`);
    return response.data;
  }
};

export default apiClient;