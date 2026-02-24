import { create } from 'zustand';

interface AppState {
  activeTab: number;
  autoRefresh: boolean;
  selectedModule: string;
  selectedVehicle: string | null;
  setActiveTab: (tab: number) => void;
  toggleAutoRefresh: () => void;
  setSelectedModule: (module: string) => void;
  setSelectedVehicle: (vehicleId: string | null) => void;
}

export const useStore = create<AppState>((set) => ({
  activeTab: 0, // 0: Ops, 1: Inference, 2: Gold, 3: Alerts, 4: Replay
  autoRefresh: true,
  selectedModule: 'engine', 
  selectedVehicle: null,
  setActiveTab: (tab) => set({ activeTab: tab }),
  toggleAutoRefresh: () => set((state) => ({ autoRefresh: !state.autoRefresh })),
  setSelectedModule: (module) => set({ selectedModule: module }),
  setSelectedVehicle: (vehicleId) => set({ selectedVehicle: vehicleId }),
}));