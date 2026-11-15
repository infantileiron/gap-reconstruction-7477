import {request} from './client';import type {StreamDetail,StreamReceipt,StreamSummary,StreamNotification} from '../types/entities';
export const list=()=>request<StreamSummary[]>('/streams');
export const detail=(station:string)=>request<StreamDetail>('/streams/'+encodeURIComponent(station));
export const notifications=(station:string)=>request<StreamNotification[]>('/streams/'+encodeURIComponent(station)+'/notifications');
export const ingest=(data:unknown)=>request<StreamReceipt>('/streams/ingest','POST',data);
export const preview=(data:unknown)=>request<{replayed:boolean;window:{first_time:number;last_time:number};count:number;stations:StreamReceipt['stations']}>('/streams/preview','POST',data);
