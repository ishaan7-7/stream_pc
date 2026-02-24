import { useMemo } from 'react';
import { Box, Typography, Paper, Chip } from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-quartz.css'; // Updated to modern Quartz theme
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';

const fetchWriterMetrics = async () => {
  const { data } = await axios.get('http://127.0.0.1:8005/api/writer/metrics');
  return data;
};

export default function WriterOps() {
  const { autoRefresh } = useStore();

  const { data, isLoading, isError } = useQuery({
    queryKey: ['writerMetrics'],
    queryFn: fetchWriterMetrics,
    refetchInterval: autoRefresh ? 2000 : false,
  });

  const rowData = useMemo(() => {
    if (!data) return [];
    return Object.keys(data).map((moduleName) => {
      // Safely extract the nested streams data from the JSON
      const modData = data[moduleName] || {};
      const stream = modData.streams?.[moduleName] || {};
      
      return {
        module: moduleName.toUpperCase(),
        status: modData.status || 'OFFLINE',
        throughput: stream.input_rate ? parseFloat(stream.input_rate).toFixed(1) : "0.0",
        processed: stream.process_rate ? parseFloat(stream.process_rate).toFixed(1) : "0.0",
        latency: stream.duration_ms || 0
      };
    });
  }, [data]);

  const columnDefs = useMemo<ColDef[]>(() => [
    { field: 'module', headerName: 'Subsystem', sortable: true, filter: true, flex: 1 },
    { 
      field: 'status', 
      headerName: 'Status', 
      cellRenderer: (params: any) => (
        <Chip 
          label={params.value} 
          color={params.value === 'RUNNING' ? "success" : "error"} 
          size="small" 
          sx={{ borderRadius: 0, fontWeight: 'bold' }} 
        />
      )
    },
    { field: 'throughput', headerName: 'Throughput (rows/s)', sortable: true, flex: 1 },
    { field: 'processed', headerName: 'Processed (rows/s)', sortable: true, flex: 1 },
    { field: 'latency', headerName: 'Latency (ms)', sortable: true, flex: 1 },
  ], []);

  return (
    <Box sx={{ height: 'calc(100vh - 100px)', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6" color="primary" sx={{ borderBottom: '2px solid #2c3e50', pb: 1 }}>
        System Operations: Writer Pipeline
      </Typography>
      
      {isError && <Typography color="error">Error connecting to Master Backend.</Typography>}
      
      <Paper sx={{ flexGrow: 1, overflow: 'hidden', borderRadius: 0, p: 1, display: 'flex', flexDirection: 'column' }}>
        <Box className="ag-theme-quartz" sx={{ flexGrow: 1, width: '100%', minHeight: '500px' }}>
          <AgGridReact
            rowData={rowData}
            columnDefs={columnDefs}
            rowSelection="single"
            animateRows={true}
            defaultColDef={{ resizable: true, sortable: true }}
            overlayLoadingTemplate={isLoading ? '<span class="ag-overlay-loading-center">Loading Metrics...</span>' : undefined}
          />
        </Box>
      </Paper>
    </Box>
  );
}