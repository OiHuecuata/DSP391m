import {DataOperations,type OperationsView} from "@/features/data-ops/DataOperations";
export default async function DataOperationsPage({searchParams}:{searchParams:Promise<{source?:string}>}){const {source}=await searchParams;const view=source&&['threads','youtube','pbplus','storage','tiktok'].includes(source)?source as OperationsView:'overview';return <DataOperations view={view}/>;}
