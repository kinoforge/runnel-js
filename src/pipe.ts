export class Pipe<T> implements AsyncIterable<T> {
  #items: T[] = [];
  #waiting: (() => void) | undefined;
  #closed = false;

  push(value: T): void {
    if (this.#closed) {
      return;
    }
    this.#items.push(value);
    this.#wake();
  }

  close(): void {
    this.#closed = true;
    this.#wake();
  }

  #wake(): void {
    const wake = this.#waiting;
    this.#waiting = undefined;
    wake?.();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<T, undefined, undefined> {
    for (;;) {
      const next = this.#items.shift();
      if (next !== undefined) {
        yield next;
        continue;
      }
      if (this.#closed) {
        return undefined;
      }
      await new Promise<void>((resolve) => {
        this.#waiting = resolve;
      });
    }
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const done = (): void => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
  });
}
