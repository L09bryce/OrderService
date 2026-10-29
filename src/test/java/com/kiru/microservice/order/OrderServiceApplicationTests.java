package com.kiru.microservice.order;

import com.kiru.microservice.order.stubs.InventoryClientStub;
import io.restassured.RestAssured;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.testcontainers.containers.MySQLContainer;

import static org.hamcrest.MatcherAssert.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWireMock(port = 0)
class OrderServiceApplicationTests {

	@ServiceConnection
	static MySQLContainer mySQLContainer = new MySQLContainer("mysql:8.3.0");
	@LocalServerPort
	private Integer port;

	@BeforeEach
	void setup() {
		RestAssured.baseURI = "http://localhost";
		RestAssured.port = port;
	}

	static {
		mySQLContainer.start();
	}

	@Test
	void shouldSubmitOrder() {
		String submitOrderJson = """
                {
                     "skuCode": "OnePlus",
                     "price": 1000,
                     "quantity": 1,
                     "userDetails": {
                         "email": "customer@example.com"
                     }
                }
                """;

		InventoryClientStub.stubInventoryCall("OnePlus", 1);
		var responseBodyString = RestAssured.given()
				.contentType("application/json")
				.body(submitOrderJson)
				.when()
				.post("/api/order")
				.then()
				.log().all()
				.statusCode(201)
				.extract()
				.body().asString();

		assertThat(responseBodyString, Matchers.is("Order Placed Successfully"));

		RestAssured.given()
				.queryParam("userEmail", "customer@example.com")
				.when()
				.get("/api/order")
				.then()
				.statusCode(200)
				.body("size()", Matchers.is(1))
				.body("[0].userEmail", Matchers.is("customer@example.com"))
				.body("[0].skuCode", Matchers.is("OnePlus"));
	}

	@Test
	void shouldReturnNoOrdersForUnknownUser() {
		RestAssured.given()
				.queryParam("userEmail", "unknown@example.com")
				.when()
				.get("/api/order")
				.then()
				.statusCode(200)
				.body("size()", Matchers.is(0));
	}
}