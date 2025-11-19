
import asyncio
import time
from workflows import (
    Workflow,
    step,
    Context,
)
from workflows.events import StartEvent, StopEvent, Event

class TriggerEvent(Event):
    pass


class TimeoutTestWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
        start_time = time.time()
        print(f"[{start_time:.2f}] Step started. Waiting for TriggerEvent with timeout=10s...")
        
        try:
            # Wait for an event that will never come
            await ctx.wait_for_event(TriggerEvent, timeout=10)
            elapsed = time.time() - start_time
            print(f"[{time.time():.2f}] Received event after {elapsed:.2f}s (Unexpected!)")
            return StopEvent(result="Received event")
        except TimeoutError as e:
            elapsed = time.time() - start_time
            print(f"[{time.time():.2f}] Caught TimeoutError after {elapsed:.2f}s as expected!")
            print(f"Error message: {e}")
            return StopEvent(result="Timeout occurred")


async def main():
    start_time = time.time()
    wf = TimeoutTestWorkflow(timeout=20, verbose=True)
    
    print(f"[{start_time:.2f}] Starting workflow...")
    handler = wf.run()
    
    result = await handler
    total_elapsed = time.time() - start_time
    print(f"[{time.time():.2f}] Workflow finished after {total_elapsed:.2f}s with result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
