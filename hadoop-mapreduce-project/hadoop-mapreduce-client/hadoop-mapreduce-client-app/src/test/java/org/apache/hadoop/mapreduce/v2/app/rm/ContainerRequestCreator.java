package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;

import java.util.Collections;
import java.util.Map;

class ContainerRequestCreator {

  public static class TestResourceRequest {
    private final int memory;
    private final int vCores;
    private final Map<String, String> customResources;

    private TestResourceRequest(int memory, int vCores,
            Map<String, String> customResources) {
      this.memory = memory;
      this.vCores = vCores;
      this.customResources = customResources;
    }

    public static TestResourceRequest createWithMemory(int memory) {
      return new TestResourceRequest(memory, 1, Collections.emptyMap());
    }

    public static TestResourceRequest create(int memory, int vCores) {
      return new TestResourceRequest(memory, vCores, Collections.emptyMap());
    }

    public static TestResourceRequest create(int memory,
            Map<String, String> customResources) {
      return new TestResourceRequest(memory, 1, customResources);
    }

    public static TestResourceRequest create(int memory, int vCores,
            Map<String, String> customResources) {
      return new TestResourceRequest(memory, vCores, customResources);
    }
  }

  static ContainerRequestEvent createRequest(JobId jobId, int taskAttemptId,
          TestResourceRequest resourceRequest, String[] hosts) {
    return createRequest(jobId, taskAttemptId, resourceRequest, hosts,
            false, false);
  }

  static ContainerRequestEvent createRequest(JobId jobId, int taskAttemptId,
          TestResourceRequest resourceRequest,
          String[] hosts, boolean earlierFailedAttempt, boolean reduce) {
    final TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
            taskAttemptId);

    final Resource containerNeed = Resource.newInstance(
            resourceRequest.memory, resourceRequest.vCores);
    for (Map.Entry<String, String> customResource :
            resourceRequest.customResources.entrySet()) {
      ResourceInformation resourceInformation = ResourceTypesTestHelper
              .createResourceInformation(customResource.getKey(), customResource
                      .getValue());
      containerNeed.setResourceInformation(customResource.getKey(),
              resourceInformation);
    }

    if (earlierFailedAttempt) {
      return ContainerRequestEvent
              .createContainerRequestEventForFailedContainer(attemptId,
                      containerNeed);
    }
    return new ContainerRequestEvent(attemptId, containerNeed, hosts,
            new String[]{NetworkTopology.DEFAULT_RACK});
  }
}
