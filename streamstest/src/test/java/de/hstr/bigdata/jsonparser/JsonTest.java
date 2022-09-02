package de.hstr.bigdata.jsonparser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import de.hstr.bigdata.jsonparser.pojo.AuthorPOJO;
import de.hstr.bigdata.jsonparser.pojo.BookPOJO;
import de.hstr.bigdata.jsonparser.pojo.DayPOJO;
import de.hstr.bigdata.jsonparser.pojo.SimpletestCaseJsonPOJO;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.SortedMap;

import static org.junit.jupiter.api.Assertions.*;

class JsonTest {

    private String simpleTestCaseJsonSource = "{  \n" +
            "  \"title\": \"Coder From Scratch\",\n" +
            "  \"author\": \"Max\"\n" +
            "}";
    private String dayScenario1 = "{  \n" +
            "  \"date\": \"2019-12-25\",\n" +
            "  \"name\": \"Christmas Day\"\n" +
            "}";
    private String authorBookScenario ="{\n" +
            "  \"authorName\": \"Max\",\n" +
            "  \"books\": [\n" +
            "    {\n" +
            "      \"title\": \"title1\",\n" +
            "      \"inPrint\": true,\n" +
            "      \"publishDate\": \"2019-12-25\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"title\": \"title2\",\n" +
            "      \"inPrint\": false,\n" +
            "      \"publishDate\": \"2019-01-01\"\n" +
            "    }\n" +
            "\n" +
            "  ]\n" +
            "}";
    @Test
    void parse() throws IOException {

        JsonNode node = Json.parse(simpleTestCaseJsonSource);
        assertEquals(node.get("title").asText(), "Coder From Scratch");
    }
    @Test
    void fromJson() throws IOException {
        JsonNode node = Json.parse(simpleTestCaseJsonSource);
        SimpletestCaseJsonPOJO pojo = Json.fromJson(node, SimpletestCaseJsonPOJO.class);

        assertEquals(pojo.getTitle(), "Coder From Scratch");
    }

    @Test
    void toJson(){
        SimpletestCaseJsonPOJO pojo = new SimpletestCaseJsonPOJO();
        pojo.setTitle("Testing123");

        JsonNode node = Json.toJson(pojo);

        assertEquals(node.get("title").asText(), "Testing123");
    }

    @Test
    void stringify() throws JsonProcessingException {
        SimpletestCaseJsonPOJO pojo = new SimpletestCaseJsonPOJO();
        pojo.setTitle("Testing123");
        JsonNode node = Json.toJson(pojo);

        System.out.println(Json.stringify(node));
    }

    @Test
    void prettyPrint() throws JsonProcessingException {
        SimpletestCaseJsonPOJO pojo = new SimpletestCaseJsonPOJO();
        pojo.setTitle("Testing123");
        JsonNode node = Json.toJson(pojo);

        System.out.println(Json.prettyPrint(node));
    }

    @Test
    void dayTestScenario1() throws IOException {
        JsonNode node = Json.parse(dayScenario1);
        DayPOJO pojo = Json.fromJson(node, DayPOJO.class);

        assertEquals("2019-12-25", pojo.getDate().toString());
    }

    @Test
    void setAuthorBookScenario1() throws IOException {
        JsonNode node = Json.parse(authorBookScenario);
        AuthorPOJO pojo = Json.fromJson(node, AuthorPOJO.class);

        System.out.println("Author : " + pojo.getAuthorName());
        for(BookPOJO bP : pojo.getBooks()){
            System.out.println("Book : "+ bP.getTitle());
            System.out.println("is in print? : "+ bP.isInPrint());
            System.out.println("Publish Date : "+ bP.getPublishDate());
        }
    }
}