import { createBrowserRouter, RouterProvider } from 'react-router';
import { Shell } from './ls/Shell';
import { OverviewPage } from './ls/pages/OverviewPage';
import { SourcesPage } from './ls/pages/SourcesPage';
import { AgentsPage } from './ls/pages/AgentsPage';
import { PipelinesPage } from './ls/pages/PipelinesPage';
import { ValidationPage } from './ls/pages/ValidationPage';
import { ImprovePage } from './ls/pages/ImprovePage';
import { GuardPage } from './ls/pages/GuardPage';

const router = createBrowserRouter([
  {
    element: <Shell />,
    children: [
      { path: '/', element: <OverviewPage /> },
      { path: '/sources', element: <SourcesPage /> },
      { path: '/agents', element: <AgentsPage /> },
      { path: '/pipelines', element: <PipelinesPage /> },
      { path: '/validation', element: <ValidationPage /> },
      { path: '/improve', element: <ImprovePage /> },
      { path: '/guard', element: <GuardPage /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
