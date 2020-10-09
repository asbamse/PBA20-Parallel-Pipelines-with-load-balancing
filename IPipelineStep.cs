using System;
namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public interface IPipelineStep
    {
        // Input queue
        //    V
        //  Internal Multiple Tasks
        //    V
        //  Internal Multiple TaskQueue
        //    V
        //  Internal Multiplexor
        //    V
        // Output queue

        // Constructor InputQueue, OutputQueue, Action
        // TODO Find out how to add the TaskQueue til Action

        bool AddTask();
        bool RemoveTask(); // Only return after Task Removal
        int TaskAmount();
        int QueueFillLevel();
    }
}
