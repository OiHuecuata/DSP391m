import type {CollectionHistory} from "@/features/s3/status";
export type Source="threads"|"youtube"|"pickleballplus";
export type Counts=Record<Source,number>;
export type Province=Counts&{id:string;name:string};
export type MapData={heatmap?:HeatmapData;provinces:Province[];totals:Counts;matched:Counts;syncedAt:string;history?:CollectionHistory|null;interactions?:{source:"threads"|"youtube";metrics:{key:"likes"|"replies"|"reposts"|"quotes";total:number;reported:number}[]}[];courtSizes?:{label:string;count:number}[]};
export type ProvinceHeatMetrics={id:string;playingCourts:number;listingsWithCourtCount:number;threadsInteractions:number;threadsReported:number;youtubeViews:number;youtubeViewsReported:number;youtubeInteractions:number;youtubeReported:number;activity:Record<string,{threads:number;youtube:number}>};
export type HeatmapData={provinces:ProvinceHeatMetrics[];months:string[]};
