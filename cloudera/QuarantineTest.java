/*
 * Copyright (c) 2018, Cloudera Inc. All rights reserved.
 */

import java.io.File;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringJoiner;
import java.util.regex.*;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.file.*;

/**
 * This program is used to group unit test executions into categories.
 * For example, all test class or method in integrations.txt file will
 * be labeled as: 'TestJUnitCategory.IntegrationsTest' category.
 */
public class QuarantineTest {

  static Map<String, String> classAndMethodToAnnotations;
  static Map<String, String> classToMethods;
  static Map<String, File> classToFile;

  static final String IMPORT_JUNIT_IGNORE =
      "import org.junit.Ignore;";

  static final String IMPORT_JUNIT_EXPERIMENTAL_CATEGORY =
      "import org.junit.experimental.categories.Category;";

  static final String IMPORT_TEST_CATEGORY =
      "import org.apache.hadoop.classification.TestJUnitCategory;";

  static final Logger LOG = Logger.getLogger(QuarantineTest.class.getName());
  static final String LINE_SEPARATOR = System.getProperty ("line.separator");
  static boolean ignoreFlakyTest = false;
  static String hadoopRoot = System.getProperty("user.dir");
  static File fileFullPath = null;

  public static void main(String[] args) {
    if (args.length > 3) {
      printHelp();
      System.exit(1);
    } else {
      // program usage: "QuarantineTest [-path hadoop_root_dir] [-i]"
      parseCommandLineOptions(args);
    }

    String categoriesPath = String.format("%s/cloudera/categories.txt",
        hadoopRoot);
    File categoriesFile = new File(categoriesPath);

    // read categories.txt file to get a list of category files
    List<File> categoryFileList = new ArrayList<>();
    try (FileReader fileReader1 = new FileReader(categoriesFile);
         BufferedReader bufferedReader = new BufferedReader(fileReader1)) {
      String category;
      while ((category = bufferedReader.readLine()) != null) {
        File item = new File(String.format("%s/cloudera/%s",
            hadoopRoot, category));
        categoryFileList.add(item);
      }
    } catch (FileNotFoundException e) {
      LOG.log(Level.SEVERE, "Cannot find categories.txt file.", e);
      System.exit(1);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error reading categories.txt file.", e);
      System.exit(1);
    }

    if (categoryFileList.isEmpty()) {
      LOG.log(Level.INFO, "Category file is empty, do nothing.");
      System.exit(0);
    }

    // process each file on the list
    for (File categoryFile : categoryFileList) {
      if (!categoryFile.exists() || categoryFile.isDirectory()) {
        LOG.log(Level.WARNING, "File does not exist or is a directory.");
        continue;
      }

      classToFile = new HashMap<>();
      classAndMethodToAnnotations = new HashMap<>();
      classToMethods = new HashMap<>();
      try (FileReader fileReader = new FileReader(categoryFile);
           BufferedReader bufferedReader = new BufferedReader(fileReader)) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          if (line.contains("/")) {
            parseCategoryEntry(line, categoryFile.getName());
          }
        }

        try {
          for (Map.Entry<String, File> entry : classToFile.entrySet()) {
            modifyAnnotation(entry.getValue(),
                classToMethods.get(entry.getKey()));
          }
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Error modifying annotation for entries " +
              "from category file: " + categoryFile.getName(), e);
          System.exit(1);
        }
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Error processing category file: "
            + categoryFile.getName(), e);
        System.exit(1);
      }
    }
  }

  /**
   * Parse each entry from the corresponding file.
   * Entry sample from cloudera/flakies.txt:
   * org/apache/hadoop/yarn/server/nodemanager/TestDirectoryCollection
   * This means test class 'TestDirectoryCollection' is under flakies category
   * and @Ignore will be applied to the test class.
   *
   * Entry sample from cloudera/integrations.txt:
   * org/apache/hadoop/hdfs/server/federation/router/TestRouterRpc#testRpcService
   * This means method 'testRpcService' in class 'TestRouterRpc' will be applied
   * @Category(TestJUnitCategory.IntegrationsTest.class) annotation.
   *
   * Note, entries in these files can also carry multiple ';' seperated
   * annotations in the format like this:
   * org/apache/hadoop/hdfs/server/federation/router/TestRouterRpc#testRpcService:
   * additional_annotation1;additional_annotation2
   * However, this feature is not used currently.
   * @param line Content of the line
   * @param shortFileName Name of the file contains the line entry
   * @return The source file name
   */
  private static void parseCategoryEntry(String line, String shortFileName) {
    // fileName equals className
    String fileName;
    String annotations;

    if (ignoreFlakyTest && shortFileName.contains("flakies")) {
      // skip(ignore) entries from flakies.txt when option "-i" is provided
      annotations = "@Ignore";
    } else {
      String categoryName = shortFileName.substring(0,
          shortFileName.lastIndexOf("."));
      categoryName = categoryName.substring(0, 1).toUpperCase() +
          categoryName.substring(1);
      annotations = "@Category(TestJUnitCategory."+categoryName+"Test.class)";
    }

    // parse entry from line end backwards
    String method = "";
    String[] arrayOfLineSplit = null;

    // Do we have additional annotations?
    if (line.contains(":")) {
      arrayOfLineSplit = line.split(":");
      annotations = annotations + ";" + arrayOfLineSplit[1];
      line = line.substring(0, line.lastIndexOf(":"));
    }

    // Is this at method level?
    if (line.contains("#")) {
      arrayOfLineSplit = line.split("#");
      method = arrayOfLineSplit[1];
      line = line.substring(0, line.lastIndexOf("#"));
    }
    fileName = line.substring(line.lastIndexOf("/") + 1);

    // fill in classAndMethodToAnnotations
    if (method == null || method.isEmpty()) {
      classAndMethodToAnnotations.put(line, annotations);
    } else {
      classAndMethodToAnnotations.put(line + "#" + method, annotations);
    }

    // fill in classToMethods map
    if (!classToMethods.containsKey(line)) {
      classToMethods.put(line, method);
    } else {
      String existingMethods = classToMethods.get(line) + ";";
      classToMethods.put(line, existingMethods + method);
    }

    // fill in classToFile map
    final String filePathWithExt = line + ".java";
    try {
      Files.walkFileTree(Paths.get(hadoopRoot), new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            throws IOException {
          if (file.toAbsolutePath().toString().contains(filePathWithExt)) {
            fileFullPath = file.toFile();
          }
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error walking directory: " + hadoopRoot);
      fileFullPath = null;
      System.exit(1);
    }

    if (fileFullPath != null) {
      classToFile.put(line, fileFullPath);
    }
    fileFullPath = null;
  }

  private static void modifyAnnotation(File testFile, String methods)
      throws IOException {
    String className = testFile.getName().substring(0,
        testFile.getName().indexOf(".java"));
    String packageLine = "(\\s)*^package(\\s)+.*";
    String classLine = "(\\s)*public(\\s)*class(\\s)*" + className + "(\\s)+.*";

    boolean insertImport = true;
    StringJoiner buf = new StringJoiner(LINE_SEPARATOR);

    String line;
    List<String> methodList = null;
    List<String> searchLines = new ArrayList<>();
    if (methods == null || methods.isEmpty()) {
      // default is class level annotation
      searchLines.add(classLine);
    } else {
      // method level annotation is requested.
      methodList = Arrays.asList(methods.split(";"));
      for (String method : methodList) {
        searchLines.add("(\\s)*public(\\s)*void(\\s)*" + method + "\\(\\).*");
      }
    }

    // process current file
    try (BufferedReader in = new BufferedReader(new FileReader(testFile))) {
      while ((line = in.readLine()) != null) {
        if (insertImport && Pattern.matches(packageLine, line)) {
          buf.add(line);
          buf.add(LINE_SEPARATOR);
          buf.add(IMPORT_JUNIT_IGNORE);
          buf.add(IMPORT_JUNIT_EXPERIMENTAL_CATEGORY);
          buf.add(IMPORT_TEST_CATEGORY);
          insertImport = false;
        }
        else {
          for (String searchLine : searchLines)
          {
            // found line we want to annotate
            if (Pattern.matches(searchLine, line))
            {
              String methodName = "";
              if (!searchLine.contains("(\\s)*public(\\s)*class(\\s)*")) {
                String methodLine = searchLine.split("\\*")[3];
                methodName = methodLine.substring(0, methodLine.indexOf('\\'));
              }

              String annotations = "";
              String key = testFile.getAbsolutePath().substring(testFile.getAbsolutePath().lastIndexOf("org/apache/hadoop/"));
              key = key.substring(0, key.indexOf(".java"));
              if (!methodName.isEmpty()) {
                key += "#" + methodName;
              }
              annotations = classAndMethodToAnnotations.get(key);

              if (annotations != null) {
                String[] annotationList = annotations.split(";");
                if (annotationList.length > 0) {
                  for (String annotation : annotationList) {
                    buf.add(annotation);
                  }
                }
              }
            }
          }
          buf.add(line);
        }
      }
    }

    // write file
    try (BufferedWriter out = new BufferedWriter(new FileWriter(testFile))) {
      out.write(buf.toString());
    }
  }

  private static void printHelp() {
    System.err.println("USAGE DESCRIPTION :");
    System.err.println( "java QuarantineTest [-path hadoop_root_dir] [-i]");
  }

  private static void parseCommandLineOptions(String[] args) {
    if (args.length > 0) {
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "-i":
            ignoreFlakyTest = true;
            break;
          case "-path":
            if (i+1 < args.length) {
              // hadoop root: ${TEMP_ROOT}/src/CDH/hadoop
              hadoopRoot = args[i+1];
            } else {
              System.err.println( "hadoop_root_dir is required after -path");
              printHelp();
              System.exit(1);
            }
            break;
          default:
            break;
        }
      }
    } else {
      // No hadoop root path provided, use user working directory
      LOG.log(Level.INFO, "No hadoop root dir was provided, user.dir - " +
          "'User working directory' will be used.");
    }
  }
}