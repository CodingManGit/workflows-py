import asyncio
import time
from workflows import (
    Workflow,
    step,
    Context,
)
from workflows.events import StartEvent, StopEvent, Event

class InputRequiredEvent(Event):
    """Event to indicate that human input is required."""
    prefix: str

class HumanResponseEvent(Event):
    """Event containing human response."""
    response: str

class MyEvent(Event):
    """Initial event to trigger the workflow."""
    query: str

class NextStepEvent(Event):
    """Event to continue to the next step."""
    approved: bool

class AgentStream(Event):
    """Event to stream agent output."""
    delta: str


class HumanInLoopWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> MyEvent:
        """Start the workflow with a query."""
        # For demo, use a fixed query
        return MyEvent(query="Test task for approval")
    
    @step
    async def process_step(self, ctx: Context, ev: MyEvent) -> NextStepEvent:
        """Process something and wait for human approval."""
        # Simulate processing
        print(f"[{time.time():.2f}] Processing query: {ev.query}")
        await asyncio.sleep(1)  # Simulate work
        
        result = f"Processed: {ev.query}"
        
        # Stream some output
        ctx.write_event_to_stream(AgentStream(delta=f"Intermediate result: {result}\n"))
        
        # Pause workflow and wait for human approval
        print(f"[{time.time():.2f}] Waiting for human approval...")
        response = await ctx.wait_for_event(
            HumanResponseEvent,
            waiter_id="approval_needed",
            waiter_event=InputRequiredEvent(
                prefix=f"Approve this action? Result: {result}\n(yes/no): "
            ),
            timeout=30  # 30 second timeout
        )
        
        return NextStepEvent(approved=response.response.lower() == "yes")
    
    @step
    async def finalize_step(self, ctx: Context, ev: NextStepEvent) -> StopEvent:
        """Finalize based on approval decision."""
        if ev.approved:
            ctx.write_event_to_stream(AgentStream(delta="Action approved! Finalizing...\n"))
            await asyncio.sleep(0.5)
            return StopEvent(result="Task completed successfully")
        else:
            ctx.write_event_to_stream(AgentStream(delta="Action rejected by user.\n"))
            return StopEvent(result="Task rejected by user")


async def main():
    print("=" * 60)
    print("Human-in-the-Loop Workflow Test")
    print("=" * 60)
    
    wf = HumanInLoopWorkflow(verbose=True)
    handler = wf.run()
    
    print(f"[{time.time():.2f}] Workflow started...")
    
    try:
        async for event in handler.stream_events():
            if isinstance(event, InputRequiredEvent):
                # Workflow is TRULY paused here
                print("\n" + "=" * 40)
                print("WORKFLOW PAUSED - INPUT REQUIRED")
                print("=" * 40)
                print(event.prefix, end="")
                
                # Simulate user input after a short delay
                await asyncio.sleep(1)
                user_input = "yes"  # Simulate user typing "yes"
                print(user_input)
                
                # Resume workflow by sending response
                print(f"[{time.time():.2f}] Sending human response: {user_input}")
                handler.ctx.send_event(HumanResponseEvent(response=user_input))
                print("=" * 40)
                print("WORKFLOW RESUMED")
                print("=" * 40 + "\n")
            
            elif isinstance(event, AgentStream):
                print(event.delta, end="", flush=True)
        
        result = await handler
        print(f"\n[{time.time():.2f}] Workflow finished with result: {result}")
        
    except TimeoutError as e:
        print(f"\n[{time.time():.2f}] Workflow timed out: {e}")
    except Exception as e:
        print(f"\n[{time.time():.2f}] Workflow error: {e}")


if __name__ == "__main__":
    asyncio.run(main())