export interface IntegrationEventProps<T>{eventId:string;aggregateId:string;correlationId:string;causationId?:string;occurredAt:Date;data:T}
export abstract class IntegrationEvent<T>{abstract readonly eventType:string; abstract readonly version:number; readonly eventId!:string; readonly aggregateId!:string; readonly correlationId!:string; readonly causationId?:string; readonly occurredAt:Date; readonly data!:Readonly<T>;
  protected constructor(p:IntegrationEventProps<T>){Object.assign(this,p);this.occurredAt=new Date(p.occurredAt)}
  toJSON(){return {eventId:this.eventId,eventType:this.eventType,aggregateId:this.aggregateId,correlationId:this.correlationId,causationId:this.causationId,occurredAt:this.occurredAt.toISOString(),version:this.version,data:this.data}}
}
export class WagerTransactionProcessed extends IntegrationEvent<Record<string,unknown>>{readonly eventType="WagerTransactionProcessed";readonly version=1}
export class WagerTransactionRejected extends IntegrationEvent<Record<string,unknown>>{readonly eventType="WagerTransactionRejected";readonly version=1}
export class WagerTransactionPendingReference extends IntegrationEvent<Record<string,unknown>>{readonly eventType="WagerTransactionPendingReference";readonly version=1}
export class WalletBalanceChanged extends IntegrationEvent<Record<string,unknown>>{readonly eventType="WalletBalanceChanged";readonly version=1}
