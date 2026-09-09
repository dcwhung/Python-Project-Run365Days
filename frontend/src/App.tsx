import { RouterProvider } from "react-router-dom";
import { DataProvider } from "@/data/DataProvider";
import { router } from "@/app/router";

export function App() {
  return (
    <DataProvider>
      <RouterProvider router={router} />
    </DataProvider>
  );
}
