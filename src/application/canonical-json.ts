import { createHash } from "node:crypto";
export function canonicalize(value:unknown):string { if(value===null||typeof value!=="object") return JSON.stringify(value); if(Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`; return `{${Object.entries(value as Record<string,unknown>).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>`${JSON.stringify(k)}:${canonicalize(v)}`).join(",")}}`; }
export function payloadHash(value:unknown):string { return createHash("sha256").update(canonicalize(value)).digest("hex"); }
