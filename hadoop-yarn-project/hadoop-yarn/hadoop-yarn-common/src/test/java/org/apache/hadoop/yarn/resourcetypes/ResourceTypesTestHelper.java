package org.apache.hadoop.yarn.resourcetypes;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResourceTypesTestHelper {

  private static final RecordFactory recordFactory = RecordFactoryProvider
          .getRecordFactory(null);

  private static class ResourceValueAndUnit {
    private final Long value;
    private final String unit;

    private ResourceValueAndUnit(Long value, String unit) {
      this.value = value;
      this.unit = unit;
    }
  }

  public static Resource newResource(long memory, int vCores, Map<String,
          String> customResources) {
    Resource resource = recordFactory.newRecordInstance(Resource.class);
    resource.setMemorySize(memory);
    resource.setVirtualCores(vCores);

    for (Map.Entry<String, String> customResource :
            customResources.entrySet()) {
      String resourceName = customResource.getKey();
      ResourceInformation resourceInformation =
              createResourceInformation(resourceName,
                      customResource.getValue());
      resource.setResourceInformation(resourceName, resourceInformation);
    }
    return resource;
  }

  public static ResourceInformation createResourceInformation(String
          resourceName, String descriptor) {
    ResourceValueAndUnit resourceValueAndUnit =
            getResourceValueAndUnit(descriptor);
    return ResourceInformation
            .newInstance(resourceName, resourceValueAndUnit.unit,
                    resourceValueAndUnit.value);
  }

  private static ResourceValueAndUnit getResourceValueAndUnit(String val) {
    String patternStr = "(\\d+)([A-za-z]*)";
    final Pattern pattern = Pattern.compile(patternStr);
    Matcher matcher = pattern.matcher(val);
    if (!matcher.find()) {
      throw new RuntimeException("Invalid pattern of resource descriptor: " +
              val);
    } else if (matcher.groupCount() != 2) {
      throw new RuntimeException("Capturing group count in string " +
              val + " is not 2!");
    }
    long value = Long.parseLong(matcher.group(1));

    return new ResourceValueAndUnit(value, matcher.group(2));
  }

}
