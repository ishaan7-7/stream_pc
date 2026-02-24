import { useMemo } from 'react';
import { Box, Typography, Paper } from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import { ColDef } from 'ag-grid-community'; // <-- 1. Import ColDef
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-alpine.css';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { useStore } from '../store';

const fetchGoldVehicles = async () => {
  const { data } = await axios.get('http://127.0.0.1:8005/api/gold/vehicles');
  return data;
};

export default function GoldHealth() {
  const { autoRefresh, setSelectedVehicle } = useStore();

  const { data, isLoading } = useQuery({
    queryKey: ['goldVehicles'],
    queryFn: fetchGoldVehicles,
    refetchInterval: autoRefresh ? 2000 : false,
  });

  const rowData = useMemo(() => {
    if (!data) return [];
    return Object.keys(data).map((vehicleId) => ({
      vehicle_id: vehicleId.toUpperCase(),
      ...data[vehicleId]
    }));
  }, [data]);

  // 2. Add <ColDef[]> here to satisfy TypeScript
  const columnDefs = useMemo<ColDef[]>(() => [
    { field: 'vehicle_id', headerName: 'Vehicle ID', sortable: true, filter: true, pinned: 'left' },
    { 
      field: 'overall_health', 
      headerName: 'Fleet Status', 
      sortable: true, 
      filter: true,
      cellStyle: (params: any) => {
        if (params.value === 'CRITICAL') return { backgroundColor: '#ffebee', color: '#d32f2f', fontWeight: 'bold' };
        if (params.value === 'WARNING') return { backgroundColor: '#fff8e1', color: '#ed6c02', fontWeight: 'bold' };
        return { backgroundColor: '#e8f5e9', color: '#2e7d32', fontWeight: 'bold' };
      }
    },
    { field: 'active_dtcs', headerName: 'Active DTCs', sortable: true, filter: 'agNumberColumnFilter' },
    { field: 'last_updated', headerName: 'Last Telemetry Sync', sortable: true },
  ], []);

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6" color="primary" sx={{ borderBottom: '2px solid #2c3e50', pb: 1 }}>
        Fleet Overview: Vehicle Health (Gold Tier)
      </Typography>
      
      <Paper sx={{ flexGrow: 1, overflow: 'hidden', borderRadius: 0, p: 1 }} className="ag-theme-alpine">
        <AgGridReact
          rowData={rowData}
          columnDefs={columnDefs}
          rowSelection="single"
          animateRows={true}
          defaultColDef={{ resizable: true, sortable: true, filter: true }}
          onRowClicked={(e) => setSelectedVehicle(e.data.vehicle_id)}
          overlayLoadingTemplate={isLoading ? '<span class="ag-overlay-loading-center">Scanning Fleet...</span>' : undefined}
        />
      </Paper>
    </Box>
  );
}