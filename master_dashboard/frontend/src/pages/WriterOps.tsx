import { useMemo } from 'react';
import { Box, Typography, Paper, Chip } from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import { ColDef } from 'ag-grid-community'; // <-- 1. Import ColDef
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';
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
    return Object.keys(data).map((moduleName) => ({
      module: moduleName.toUpperCase(),
      status: 'ACTIVE',
      ...data[moduleName]
    }));
  }, [data]);

  // 2. Add <ColDef[]> here
  const columnDefs = useMemo<ColDef[]>(() => [
    { field: 'module', headerName: 'Subsystem', sortable: true, filter: true, flex: 1 },
    { 
      field: 'status', 
      headerName: 'Status', 
      cellRenderer: (params: any) => (
        <Chip label={params.value} color="success" size="small" sx={{ borderRadius: 0, fontWeight: 'bold' }} />
      )
    },
    { field: 'inputRowsPerSecond', headerName: 'Throughput (rows/s)', sortable: true, filter: 'agNumberColumnFilter', flex: 1 },
    { field: 'processedRowsPerSecond', headerName: 'Processed (rows/s)', sortable: true, filter: 'agNumberColumnFilter', flex: 1 },
    { field: 'durationMs.triggerExecution', headerName: 'Latency (ms)', sortable: true, filter: 'agNumberColumnFilter', flex: 1 },
  ], []);

  return (
    <Box sx={{ height: 'calc(100vh - 100px)', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6" color="primary" sx={{ borderBottom: '2px solid #2c3e50', pb: 1 }}>
        System Operations: Writer Pipeline
      </Typography>
      
      {isError && <Typography color="error">Error connecting to Master Backend.</Typography>}
      
      {/* ADDED display: flex and a strict wrapper with minHeight */}
      <Paper sx={{ flexGrow: 1, overflow: 'hidden', borderRadius: 0, p: 1, display: 'flex', flexDirection: 'column' }}>
        <Box className="ag-theme-alpine" sx={{ flexGrow: 1, width: '100%', minHeight: '500px' }}>
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