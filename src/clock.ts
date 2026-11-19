import { timestampMs, type Timestamp } from "@bufbuild/protobuf/wkt";

export type Now = () => number;

export class ServerClock {
  #offset = 0;

  constructor(readonly local: Now = Date.now) {}

  observe(server: Timestamp | undefined): void {
    if (server && (server.seconds !== 0n || server.nanos !== 0)) {
      this.#offset = timestampMs(server) - this.local();
    }
  }

  now(): number {
    return this.local() + this.#offset;
  }

  toLocal(serverMs: number): number {
    return serverMs - this.#offset;
  }
}
