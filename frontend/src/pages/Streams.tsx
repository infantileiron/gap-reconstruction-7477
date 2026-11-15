import {useState} from 'react';
import * as api from '../api/streams';
import {useResource} from '../hooks/useResource';
import {refreshResources} from '../stores/resources';
import PageTitle from '../components/common/PageTitle';
import Notice from '../components/common/Notice';
import EmptyState from '../components/common/EmptyState';
import type {StreamDetail, StreamNotification, User} from '../types/entities';
import {decimal} from '../utils/format';
import {notificationLabels} from '../constants/enums';

const SAMPLE = 'station,time_hour,value\nLP-01,0,\nLP-01,6,NA\nLP-01,18,10.42';

function NotificationBasis({item}:{item:StreamNotification}){
  const b=item.basis;
  return <div className="basis-panel">
    <dl className="basis-grid">
      <div><dt>通知类型</dt><dd>{notificationLabels[item.kind]||item.kind}</dd></div>
      <div><dt>触发时刻 / 阈值</dt><dd>{b.triggered_at}h / {b.threshold_hours}h</dd></div>
      <div><dt>缺口起点</dt><dd>{b.gap_started_at==null?'—':b.gap_started_at+'h'}</dd></div>
      <div><dt>结算时长</dt><dd>{b.duration_hours}h</dd></div>
      <div><dt>触发前滤波状态</dt><dd>{b.filter_before?`${b.filter_before.time_hour}h · m=${decimal(b.filter_before.mean)} · P=${decimal(b.filter_before.variance,6)}`:'无（尚无实测）'}</dd></div>
      {b.filter_after&&<div><dt>恢复后滤波状态</dt><dd>{b.filter_after.time_hour}h · m={decimal(b.filter_after.mean)} · P={decimal(b.filter_after.variance,6)}</dd></div>}
      <div><dt>批次摘要</dt><dd className="mono">{b.batch_digest.slice(0,16)}…</dd></div>
      {b.alert_id&&<div><dt>对应预警</dt><dd className="mono">{b.alert_id}</dd></div>}
    </dl>
    <p className="basis-note">{b.explanation}</p>
    <div className="table-scroll"><table>
      <thead><tr><th>缺口读数小时</th><th>原始读数</th><th>补值（单侧）</th><th>状态区间</th><th>观测区间</th></tr></thead>
      <tbody>{b.gap_readings.map(p=><tr key={p.time_hour}><td>{p.time_hour}</td><td>{p.value==null?'缺失':decimal(p.value)}</td><td>{p.mean==null?'不可估':decimal(p.mean)}</td><td>{p.state_low==null?'不可估':`${decimal(p.state_low,2)} — ${decimal(p.state_high,2)}`}</td><td>{p.observation_low==null?'不可估':`${decimal(p.observation_low,2)} — ${decimal(p.observation_high,2)}`}</td></tr>)}</tbody>
    </table></div>
  </div>;
}

function StationDetail({station,user}:{station:string;user:User}){
  const {data,error}=useResource<StreamDetail|null>(()=>api.detail(station),null);
  const [open,setOpen]=useState<string|null>(null);
  if(error) return <Notice error={error}/>;
  if(!data) return <EmptyState text="站点流尚未建立。"/>;
  return <>
    <div className="stream-summary">
      <div><small>已受理至</small><strong>{data.cursor==null?'—':data.cursor+'h'}</strong></div>
      <div><small>累计缺测时长</small><strong>{decimal(data.missing_hours_total,2)}<em> h</em></strong></div>
      <div><small>当前缺口</small><b>{data.gap_active?`进行中 · 自 ${data.gap_started_at}h`:'无'}</b></div>
      <div><small>通知 超限 / 恢复</small><b>{data.counters.alerts} / {data.counters.recoveries}</b></div>
    </div>
    <section className="paper">
      <h2>通知依据</h2>
      {data.notifications.length===0?<p>尚无通知。缺测时长达到 {data.config.gap_threshold_hours}h 时产生一次超限通知，回到实测时再产生一次恢复通知。</p>:data.notifications.map(n=><article className="notification-row" key={n.id}>
        <button className="link" onClick={()=>setOpen(open===n.id?null:n.id)}>
          <span className={'badge '+n.kind}>{notificationLabels[n.kind]}</span> {n.basis.triggered_at}h · {n.duration_hours}h · {n.id}
        </button>
        {open===n.id&&<NotificationBasis item={n}/>}
      </article>)}
    </section>
    <section className="paper table-scroll">
      <h2>逐条接收留痕（{data.readings.length}）</h2>
      <table>
        <thead><tr><th>小时</th><th>原始读数</th><th>补值</th><th>状态区间 P</th><th>观测区间 P+R</th><th>状态</th><th>批次</th></tr></thead>
        <tbody>{data.readings.map(p=><tr key={p.time_hour}>
          <td>{p.time_hour}</td><td>{p.value==null?'缺失':decimal(p.value)}</td><td>{p.is_observation?'—':decimal(p.fill_value)}</td>
          <td>{p.state_low==null?'不可估':`${decimal(p.state_low,2)} — ${decimal(p.state_high,2)}`}</td>
          <td>{p.observation_low==null?'不可估':`${decimal(p.observation_low,2)} — ${decimal(p.observation_high,2)}`}</td>
          <td>{p.estimate_status==='estimable'?'可估':'不可估'}</td><td className="mono">{p.batch_digest.slice(0,8)}</td>
        </tr>)}</tbody>
      </table>
      <small>补值只由该时刻及之前已到达的读数给出；重放同批读数不会改写本表，追加未来读数也不会回改历史。</small>
    </section>
    <section className="paper">
      <h2>受理批次（{data.batches.length}）</h2>
      {data.batches.map(b=><article className="record" key={b.batch_digest}><div><b className="mono">{b.batch_digest.slice(0,16)}…</b><small>{b.first_time}—{b.last_time}h · {b.count}条 · {b.received_at}</small></div></article>)}
    </section>
  </>;
}

export default function Streams({user}:{user:User}){
  const {data:streams,error}=useResource(api.list,[]);
  const [station,setStation]=useState('LP-01');
  const [text,setText]=useState(SAMPLE);
  const [threshold,setThreshold]=useState(12);
  const [message,setMessage]=useState('');
  const [receipt,setReceipt]=useState('');
  const send=async(preview:boolean)=>{
    setMessage('');setReceipt('');
    try{
      const payload={text,gap_threshold_hours:threshold};
      if(preview){
        const r=await api.preview(payload);
        setReceipt(`预览（未写入）：${r.count}条，${r.window.first_time}—${r.window.last_time}h，`+r.stations.map(s=>`${s.station} 游标${s.cursor}h 新增${s.points_added} 通知${s.notifications.length}`).join('；'));
      }else{
        const r=await api.ingest(payload);
        setReceipt(r.replayed?`同批读数已受理，返回原回执 ${r.batch_digest.slice(0,12)}…（不重复通知）`:`已受理 ${r.count}条 · ${r.batch_digest.slice(0,12)}…`);
        refreshResources();
      }
    }catch(e:any){setMessage(e.message)}
  };
  return <>
    <PageTitle chapter="07" title="读数一到就判断，缺测按实际时间结算" description="逐站接收读数，只用当时已到达的数据做单侧补值与区间；首条实测前标为不可估，缺测超限与恢复各通知一次。"/>
    <Notice error={error||message}/>
    <section className="paper">
      <h2>站点流</h2>
      {streams.length===0?<EmptyState text="提交第一批读数后，这里显示逐站游标、缺测时长与通知计数。"/>:<div className="table-scroll"><table>
        <thead><tr><th>测站</th><th>已受理至</th><th>累计缺测 h</th><th>当前缺口</th><th>超限 / 恢复</th><th>读数 可估 / 不可估</th></tr></thead>
        <tbody>{streams.map(s=><tr key={s.station} className={station===s.station?'selected-row':''} onClick={()=>setStation(s.station)}>
          <td><b>{s.station}</b></td><td>{s.cursor==null?'—':s.cursor+'h'}</td><td>{decimal(s.missing_hours_total,2)}</td>
          <td>{s.gap_active?`自 ${s.gap_started_at}h`:'无'}</td><td>{s.counters.alerts} / {s.counters.recoveries}</td>
          <td>{s.counters.estimable} / {s.counters.unestimable}</td>
        </tr>)}</tbody>
      </table></div>}
    </section>
    {user.role!=='viewer'&&<form className="paper form-grid" onSubmit={e=>{e.preventDefault();send(false)}}>
      <label className="wide">读数批次 <small>station,time_hour,value；空/NA/null 表示该时刻确认缺测，0 仍是有效读数。同一站时间必须递增。</small>
        <textarea aria-label="读数批次" value={text} onChange={e=>setText(e.target.value)}/>
      </label>
      <label>缺测预警阈值（小时）<input aria-label="缺测预警阈值" type="number" min=".1" step=".5" value={threshold} onChange={e=>setThreshold(+e.target.value)}/></label>
      <div className="actions"><button type="button" onClick={()=>send(true)}>试算（不写入）</button><button className="primary">接收这批读数</button></div>
    </form>}
    {receipt&&<p className="receipt">{receipt}</p>}
    <StationDetail key={station} station={station} user={user}/>
  </>;
}
