package org.apache.hadoop.mapreduce.v2.app.rm;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event
        .TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestCreator
        .TestResourceRequest;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hadoop.mapreduce.v2.app.rm.ContainerRequestCreator
        .createRequest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRMContainerAllocatorWithCustomResourceTypes {

  static final Logger LOG = LoggerFactory
          .getLogger(TestRMContainerAllocatorWithCustomResourceTypes.class);


  private static class CustomResourceTypesConfigurationProvider
          extends LocalConfigurationProvider {

    @Override
    public InputStream getConfigurationInputStream(Configuration bootstrapConf,
            String name) throws YarnException, IOException {
      if (YarnConfiguration.RESOURCE_TYPES_CONFIGURATION_FILE.equals(name)) {
        return new ByteArrayInputStream(
                ("<configuration>\n" +
                        " <property>\n" +
                        "   <name>yarn.resource-types</name>\n" +
                        "   <value>custom-resource-1," +
                        "custom-resource-2,custom-resource-3</value>\n" +
                        " </property>\n" +
                        " <property>\n" +
                        "   <name>yarn.resource-types" +
                        ".custom-resource-1.units</name>\n" +
                        "   <value>G</value>\n" +
                        " </property>\n" +
                        " <property>\n" +
                        "   <name>yarn.resource-types" +
                        ".custom-resource-2.units</name>\n" +
                        "   <value>G</value>\n" +
                        " </property>\n" +
                        "</configuration>\n").getBytes());
      } else {
        return super.getConfigurationInputStream(bootstrapConf, name);
      }
    }
  }

  private Configuration createConfWithCustomResourceTypes() {
    Configuration configuration = new Configuration();
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
            CustomResourceTypesConfigurationProvider.class.getName());

    return configuration;
  }

  private List<JobEvent> filterJobEvents(List<JobEvent> jobEvents,
          JobEventType eventType) {
    return jobEvents.stream()
            .filter(je -> je.getType().equals(eventType))
            .collect(Collectors.toList());
  }

  @Test
  public void testCustomResourcesAreConsideredInContainerAllocation() throws
          Exception {
    LOG.info("Running testCustomResourcesAreConsideredInContainerAllocation");

    Configuration conf =
            createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    rm.drainEvents();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    rm.drainEvents();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
            .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rm.drainEvents();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
            MRBuilderUtils.newJobReport(jobId, "job", "user", JobState
                            .RUNNING, 0,
                    0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    TestRMContainerAllocator.MyContainerAllocator allocator = new
            TestRMContainerAllocator.MyContainerAllocator(rm, conf,
            appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    rm.drainEvents();

    // create a MAP container request
    ContainerRequestEvent event1 = ContainerRequestCreator.createRequest
            (jobId, 1, TestResourceRequest.create(1024,
            ImmutableMap.<String, String>builder().put(
                    "custom-resource-1", "11")
                    .build()),
            new String[]{"h1"});
    allocator.sendRequest(event1);

    // send 1 more REDUCE request with different resource
    ContainerRequestEvent event2 = createRequest(jobId, 2,
            TestResourceRequest.create(2048,
            ImmutableMap.<String, String>builder().put(
                    "custom-resource-2", "22")
                    .build()),
            new String[]{"h2"}, false, true);
    allocator.sendRequest(event2);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    rm.drainEvents();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    rm.drainEvents();

    assigned = allocator.schedule();
    rm.drainEvents();
    Assert.assertEquals("Assigned container number should be 0!", 0, assigned
            .size());

    List<JobEvent> jobKillEvents = filterJobEvents(TestRMContainerAllocator
            .MyContainerAllocator
            .jobEvents, JobEventType.JOB_KILL);
    Assert.assertEquals("Should Have 2 Job Kill Events", 2, jobKillEvents
            .size());

    List<JobEvent> jobDiagnosticUpdates = filterJobEvents
            (TestRMContainerAllocator.MyContainerAllocator
            .jobEvents, JobEventType.JOB_DIAGNOSTIC_UPDATE);
    Assert.assertEquals("Should Have 2 Job Diagnostic Events", 2,
            jobDiagnosticUpdates.size());

    String msg1 = ((JobDiagnosticsUpdateEvent)
            jobDiagnosticUpdates.get(0)).getDiagnosticUpdate();
    String msg2 = ((JobDiagnosticsUpdateEvent)
            jobDiagnosticUpdates.get(1)).getDiagnosticUpdate();

    Assert.assertEquals("The required MAP capability is more than the " +
            "supported " +
            "max container capability in the cluster. Killing the Job. " +
            "mapResourceRequest: <memory:1024, vCores:1, custom-resource-1: " +
            "11> " +
            "maxContainerCapability:<memory:10240, vCores:1>", msg1);

    Assert.assertEquals("The required REDUCE capability is more than the " +
            "supported " +
            "max container capability in the cluster. Killing the Job. " +
            "reduceResourceRequest: <memory:2048, vCores:1, " +
            "custom-resource-2: 22> " +
            "maxContainerCapability:<memory:10240, vCores:1>", msg2);
  }

  @Test
  public void
  testCustomResourcesAreConsideredInContainerAllocationWithDifferentUnits()
          throws Exception {
    LOG.info("Running " +
            "testCustomResourcesAreConsideredInContainerAllocationWithDifferentUnits");

    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    rm.drainEvents();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    rm.drainEvents();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
            .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rm.drainEvents();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
            MRBuilderUtils.newJobReport(jobId, "job", "user", JobState
                            .RUNNING, 0,
                    0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    TestRMContainerAllocator.MyContainerAllocator allocator = new
            TestRMContainerAllocator.MyContainerAllocator(rm, conf,
            appAttemptId, mockJob) {
      @Override
      protected Resource getMaxContainerCapability() {
        ImmutableMap<String, String> customResources = ImmutableMap.<String,
                String>builder()
                .put("custom-resource-1", "3G")
                .put("custom-resource-2", "4G")
                .put("custom-resource-3", "2222")
                .build();
        return ResourceTypesTestHelper.newResource(10240, 1, customResources);
      }
    };

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    rm.drainEvents();

    // create a MAP container request
    ContainerRequestEvent event1 = ContainerRequestCreator.createRequest
            (jobId, 1,
            TestResourceRequest.create(1024,
                    ImmutableMap.<String, String>builder()
                            .put("custom-resource-1", "2050M")
                            .build()),
            new String[]{"h1"});
    allocator.sendRequest(event1);

    // send a REDUCE request with different resource
    ContainerRequestEvent event2 = createRequest(jobId, 2,
            TestResourceRequest.create(2048,
            ImmutableMap.<String, String>builder().put("custom-resource-2",
                    "1030M")
                    .build()),
            new String[]{"h2"}, false, true);
    allocator.sendRequest(event2);

    allocator.setReduceResourceRequest(BuilderUtils.newEmptyResource());
    // send 1 more REDUCE request with a resource without units
    ContainerRequestEvent event3 = createRequest(jobId, 2,
            TestResourceRequest.create(2048,
            ImmutableMap.<String, String>builder().put("custom-resource-3",
                    "1050")
                    .build()),
            new String[]{"h3"}, false, true);
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    rm.drainEvents();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    rm.drainEvents();

    assigned = allocator.schedule();
    rm.drainEvents();
    Assert.assertEquals("Assigned container number should be 1!", 1, assigned
            .size());

    List<JobEvent> jobKillEvents = filterJobEvents(TestRMContainerAllocator
            .MyContainerAllocator
            .jobEvents, JobEventType.JOB_KILL);
    Assert.assertEquals("Should Have 0 Job Kill Events", 0, jobKillEvents
            .size());

    List<JobEvent> jobDiagnosticUpdates = filterJobEvents
            (TestRMContainerAllocator.MyContainerAllocator
            .jobEvents, JobEventType.JOB_DIAGNOSTIC_UPDATE);
    Assert.assertEquals("Should Have 0 Job Diagnostic Events", 0,
            jobDiagnosticUpdates.size());
  }

  @Test
  public void
  testAcceptOfCustomResourceRequestedUnitIsSmallerThanAvailableUnit() {
    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();

    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "11")
                    .build());

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "0G")
                    .build());

    boolean result = TestRMContainerAllocator.MyContainerAllocator
            .isCustomResourcesOfRequestAccepted(requestedResource,
                    availableResource);
    Assert.assertFalse(String.format("Resource request should not be accepted" +
                    ". " +
                    "Requested: %s, available: %s", requestedResource,
            availableResource), result);
  }

  @Test
  public void
  testAcceptOfCustomResourceRequestedUnitIsSmallerThanAvailableUnit2() {
    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();


    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "11")
                    .build());

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "1G")
                    .build());

    boolean result = TestRMContainerAllocator.MyContainerAllocator
            .isCustomResourcesOfRequestAccepted(requestedResource,
                    availableResource);
    Assert.assertTrue(String.format("Resource request should be accepted. " +
                    "Requested: %s, available: %s", requestedResource,
            availableResource), result);
  }

  @Test
  public void
  testAcceptOfCustomResourceRequestedUnitIsGreaterThanAvailableUnit() {
    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();


    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "1M")
                    .build());

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "120k")
                    .build());

    boolean result = TestRMContainerAllocator.MyContainerAllocator
            .isCustomResourcesOfRequestAccepted(requestedResource,
                    availableResource);
    Assert.assertFalse(String.format("Resource request should not be accepted" +
                    ". " +
                    "Requested: %s, available: %s", requestedResource,
            availableResource), result);
  }

  @Test
  public void
  testAcceptOfCustomResourceRequestedUnitIsGreaterThanAvailableUnit2() {
    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();


    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "11M")
                    .build());

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "1G")
                    .build());

    boolean result = TestRMContainerAllocator.MyContainerAllocator
            .isCustomResourcesOfRequestAccepted(requestedResource,
                    availableResource);
    Assert.assertTrue(String.format("Resource request should be accepted. " +
                    "Requested: %s, available: %s", requestedResource,
            availableResource), result);
  }

  @Test
  public void
  testAcceptOfCustomResourceRequestedUnitIsSameAsAvailableUnit() {
    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();


    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "11M")
                    .build());

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "100M")
                    .build());

    boolean result = TestRMContainerAllocator.MyContainerAllocator
            .isCustomResourcesOfRequestAccepted(requestedResource,
                    availableResource);
    Assert.assertTrue(String.format("Resource request should be accepted. " +
                    "Requested: %s, available: %s", requestedResource,
            availableResource), result);
  }

  @Test
  public void
  testAcceptOfCustomResourceRequestedUnitIsSameAsAvailableUnit2() {
    Configuration conf = createConfWithCustomResourceTypes();
    TestRMContainerAllocator.MyResourceManager rm = new
            TestRMContainerAllocator.MyResourceManager(conf);
    rm.start();

    Resource requestedResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "110M")
                    .build());

    Resource availableResource = ResourceTypesTestHelper.newResource(1, 1,
            ImmutableMap.<String,
                    String>builder()
                    .put("custom-resource-1", "100M")
                    .build());

    boolean result = TestRMContainerAllocator.MyContainerAllocator
            .isCustomResourcesOfRequestAccepted(requestedResource,
                    availableResource);
    Assert.assertFalse(String.format("Resource request should not be accepted" +
                    ". " +
                    "Requested: %s, available: %s", requestedResource,
            availableResource), result);
  }
}
