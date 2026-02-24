import { useMemo } from 'react';
import { Box, Typography, Paper } from '@mui/material';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef } from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-quartz.css'; // Updated to Quartz theme
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
    return Object.keys(data).map((vehicleId) => {
      const simData = data[vehicleId];
      
      // Calculate overall health across all active modules
      let totalHealth = 0;
      let count = 0;
      for (const mod in simData) {
        if (simData[mod] && simData[mod].health !== undefined) {
          totalHealth += simData[mod].health;
          count++;
        }
      }
      const avgHealth = count > 0 ? (totalHealth / count) : 100;
      
      let status = 'NORMAL';
      if (avgHealth < 50) status = 'CRITICAL';
      else if (avgHealth < 80) status = 'WARNING';

      return {
        vehicle_id: vehicleId.toUpperCase(),
        overall_health: status,
        score: avgHealth.toFixed(1) + '%',
      };
    });
  }, [data]);

  const columnDefs = useMemo<ColDef[]>(() => [
    { field: 'vehicle_id', headerName: 'Vehicle ID', sortable: true, filter: true, pinned: 'left', flex: 1 },
    { 
      field: 'overall_health', 
      headerName: 'Fleet Status', 
      sortable: true, 
      filter: true,
      flex: 1,
      cellStyle: (params: any) => {
        if (params.value === 'CRITICAL') return { backgroundColor: '#ffebee', color: '#d32f2f', fontWeight: 'bold' };
        if (params.value === 'WARNING') return { backgroundColor: '#fff8e1', color: '#ed6c02', fontWeight: 'bold' };
        return { backgroundColor: '#e8f5e9', color: '#2e7d32', fontWeight: 'bold' };
      }
    },
    { field: 'score', headerName: 'Health Score', sortable: true, flex: 1 },
  ], []);

  return (
    <Box sx={{ height: 'calc(100vh - 100px)', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <Typography variant="h6" color="primary" sx={{ borderBottom: '2px solid #2c3e50', pb: 1 }}>
        Fleet Overview: Vehicle Health (Gold Tier)
      </Typography>
      
      <Paper sx={{ flexGrow: 1, overflow: 'hidden', borderRadius: 0, p: 1, display: 'flex', flexDirection: 'column' }}>
        <Box className="ag-theme-quartz" sx={{ flexGrow: 1, width: '100%', minHeight: '500px' }}>
          <AgGridReact
            rowData={rowData}
            columnDefs={columnDefs}
            rowSelection="single"
            animateRows={true}
            defaultColDef={{ resizable: true, sortable: true, filter: true }}
            onRowClicked={(e) => setSelectedVehicle(e.data.vehicle_id)}
            overlayLoadingTemplate={isLoading ? '<span class="ag-overlay-loading-center">Scanning Fleet...</span>' : undefined}
          />
        </Box>
      </Paper>
    </Box>
  );
}