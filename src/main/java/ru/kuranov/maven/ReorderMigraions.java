package ru.kuranov.maven;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;


@Mojo(name = "start")
public class ReorderMigraions extends AbstractMojo {

    @Parameter(property = "url", defaultValue = "jdbc:postgresql://localhost:5432/maven")
    private String url;
    @Parameter(property = "user", defaultValue = "maven")
    private String user;
    @Parameter(property = "password", defaultValue = "maven")
    private String password;
    @Parameter(property = "schema", defaultValue = "maven")
    private String schema;


    public void execute()
            throws MojoExecutionException {
        try (Connection connection = DriverManager.getConnection(getUrl(), getUser(), getPassword());
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT SCRIPT FROM flyway_schema_history")) {
            connection.setSchema(schema);
            String projRoot = getPluginContext().get("project").toString()
                    .split(" @ ")[1].split("backend")[0];
            String dirReorderedMigrations = projRoot + "backend\\maven-db\\src\\main\\resources\\flyway\\migrations_reordered";
            new File(dirReorderedMigrations).mkdirs();
            FileUtils.cleanDirectory(dirReorderedMigrations);
            Integer lastScriptIndex = 0;
            File dirMigrations = new File(projRoot + "backend\\maven-db\\src\\main\\resources\\flyway\\migrations");
            Map<String, File> allScripts = Arrays.asList(dirMigrations.listFiles()).stream()
                    .collect(Collectors.toMap(File::getName, file -> file));
            Set<String> executedMigrations = new HashSet<>();
            while (rs.next()) {
                String executedScriptName = rs.getString("SCRIPT");
                String firstPartOfScriptIndex = executedScriptName
                        .split("__")[0];
                Integer numFirstPartOfScriptIndex = 0;
                try {
                    numFirstPartOfScriptIndex = Integer.parseInt(firstPartOfScriptIndex.split("V")[1]);
                } catch (NumberFormatException e) {
                    //Kind of normal situation
                }
                if (numFirstPartOfScriptIndex > lastScriptIndex) {
                    lastScriptIndex = numFirstPartOfScriptIndex;
                }
                if (allScripts.containsKey(getOriginalScriptName(executedScriptName, allScripts))) {
                    Files.copy(Paths.get(allScripts.get(getOriginalScriptName(executedScriptName, allScripts)).getPath()),
                            Paths.get(dirReorderedMigrations + "\\" +
                                    executedScriptName), REPLACE_EXISTING);
                } else {
                    throw new Exception("Cannot find the original script for migration:" + executedScriptName);
                }
                executedMigrations.add(rs.getString("SCRIPT"));
            }
            lastScriptIndex++;
            TreeSet<File> notExecutedScripts = new TreeSet<>(allScripts.values().stream().filter(checkingScript ->
                    !checkScriptExecuted(deleteVPrefix(checkingScript.toString()), executedMigrations))
                    .collect(Collectors.toSet()));
            for (File file : notExecutedScripts) {
                List<String> dividedFileName = new ArrayList<>(Arrays.asList(file.getName().split("__")));
                dividedFileName.set(0, "V" + lastScriptIndex.toString());
                lastScriptIndex++;
                Files.copy(Paths.get(file.getPath()),
                        Paths.get(dirReorderedMigrations + "\\" +
                                StringUtils.join(dividedFileName.toArray(), "__")),
                        REPLACE_EXISTING);
            }

        } catch (Exception e) {
            getLog().info("Unable to get current database for migration", e);
        }

    }

    private boolean checkScriptExecuted(String checkingScript, Set<String> passedMigrations) {
        boolean val = !passedMigrations.stream().noneMatch(migrationName -> migrationName
                .contains(checkingScript));
        return val;
    }

    private String getOriginalScriptName(String dbScriptName, Map<String, File> allScripts) {
        String reducedDbScriptName = deleteVPrefix(dbScriptName);
        return allScripts.keySet().stream().filter(name -> name.contains(reducedDbScriptName)).findFirst().get();
    }

    private String deleteVPrefix(String origString) {
        LinkedList splitedString = new LinkedList(Arrays.asList(origString.split("__")));
        splitedString.remove();
        return StringUtils.join(splitedString.toArray(), "__");
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
