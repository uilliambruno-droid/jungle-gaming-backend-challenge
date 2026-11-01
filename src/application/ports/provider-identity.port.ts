export interface ProviderIdentity {
  readonly providerId: string;
  readonly subject: string;
  readonly scopes: readonly string[];
}

export interface ProviderIdentityPort {
  authenticateBearerToken(token: string): Promise<ProviderIdentity | null>;
}
