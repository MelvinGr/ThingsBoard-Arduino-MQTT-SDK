/*
  ThingsBoard.h - Library API for sending data to the ThingsBoard
  Based on PubSub MQTT library.
  Created by Olender M. Oct 2018.
  Released into the public domain.
*/
#ifndef ThingsBoard_h
#define ThingsBoard_h

#ifndef ESP8266
#include <ArduinoHttpClient.h>
#endif

#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <ArduinoJson/Polyfills/type_traits.hpp>
#include <vector>

#define Default_Payload 64
#define Default_Fields_Amt 8

class ThingsBoardDefaultLogger;

// Telemetry record class, allows to store different data using common interface.
class Telemetry {
	template <size_t PayloadSize = Default_Payload,
		size_t MaxFieldsAmt = Default_Fields_Amt,
		typename Logger = ThingsBoardDefaultLogger>
		friend class ThingsBoardSized;

#ifndef ESP8266
	template <size_t PayloadSize = Default_Payload,
		size_t MaxFieldsAmt = Default_Fields_Amt,
		typename Logger = ThingsBoardDefaultLogger>
		friend class ThingsBoardHttpSized;
#endif

public:
	inline Telemetry()
		:m_type(TYPE_NONE), m_key(nullptr), m_value() { }

	// Constructs telemetry record from integer value.
	// EnableIf trick is required to overcome ambiguous float/integer conversion
	template<
		typename T,
		typename = ARDUINOJSON_NAMESPACE::enable_if<ARDUINOJSON_NAMESPACE::is_integral<T>::value>>
		inline Telemetry(const char* key, T val)
		:m_type(TYPE_INT), m_key(key), m_value() {
		m_value.integer = val;
	}

	// Constructs telemetry record from boolean value.
	inline Telemetry(const char* key, bool val)
		: m_type(TYPE_BOOL), m_key(key), m_value() {
		m_value.boolean = val;
	}

	// Constructs telemetry record from float value.
	inline Telemetry(const char* key, float val)
		: m_type(TYPE_REAL), m_key(key), m_value() {
		m_value.real = val;
	}

	// Constructs telemetry record from string value.
	inline Telemetry(const char* key, const char* val)
		: m_type(TYPE_STR), m_key(key), m_value() {
		m_value.str = val;
	}

private:
	// Data container
	union data {
		const char* str;
		bool        boolean;
#if ARDUINOJSON_HAS_INT64
		int64_t     integer;
#else
		int32_t     integer;
#endif
		float       real;
	};

	// Data type inside a container
	enum dataType {
		TYPE_NONE,
		TYPE_BOOL,
		TYPE_INT,
		TYPE_REAL,
		TYPE_STR,
	};

	dataType     m_type;	// Data type flag
	const char* m_key;		// Data key
	data         m_value;	// Data value

	// Serializes key-value pair in a generic way.
	bool serializeKeyval(JsonVariant& jsonObj) const {
		if (m_key) {
			switch (m_type) {
			case TYPE_BOOL:
				jsonObj[m_key] = m_value.boolean;
				break;
			case TYPE_INT:
				jsonObj[m_key] = m_value.integer;
				break;
			case TYPE_REAL:
				jsonObj[m_key] = m_value.real;
				break;
			case TYPE_STR:
				jsonObj[m_key] = m_value.str;
				break;
			default:
				break;
			}
		}
		else {
			switch (m_type) {
			case TYPE_BOOL:
				return jsonObj.set(m_value.boolean);
			case TYPE_INT:
				return jsonObj.set(m_value.integer);
			case TYPE_REAL:
				return jsonObj.set(m_value.real);
			case TYPE_STR:
				return jsonObj.set(m_value.str);
			default:
				break;
			}
		}
		return true;
	}
};

// Convenient aliases
using Attribute = Telemetry;
using RPC_Response = Telemetry;
// JSON object is used to communicate RPC parameters to the client
using RPC_Data = JsonVariant;

// RPC callback wrapper
class RPC_Callback {
	template <size_t PayloadSize, size_t MaxFieldsAmt, typename Logger>
	friend class ThingsBoardSized;

public:
	// RPC callback signature
	using processFn = RPC_Response(*)(const RPC_Data & data);

	// Constructs empty callback
	inline RPC_Callback()
		:m_name(), m_cb(nullptr) {  }

	// Constructs callback that will be fired upon a RPC request arrival with
	// given method name
	inline RPC_Callback(const char* methodName, processFn cb)
		: m_name(methodName), m_cb(cb) {  }

private:
	const char* m_name;    // Method name
	processFn   m_cb;      // Callback to call
};

class ThingsBoardDefaultLogger
{
public:
	static void log(const char* msg) {
#ifndef THINGSBOARD_NO_LOG
		Serial.print(F("[TB] "));
		Serial.println(msg);
#endif
	}
};

// ThingsBoardSized client class
template <size_t PayloadSize, size_t MaxFieldsAmt, typename Logger>
class ThingsBoardSized
{
public:
	// Initializes ThingsBoardSized class with network client.
	inline ThingsBoardSized(Client& client) :m_client(client) { }

	// Destroys ThingsBoardSized class with network client.
	inline ~ThingsBoardSized() { }

	// Connects to the specified ThingsBoard server and port.
	// Access token is used to authenticate a client.
	// Returns true on success, false otherwise.
	bool connect(const char* host, const char* access_token, uint16_t port = 1883) {
		if (!host || !access_token)
			return false;

		RPC_Unsubscribe(); // Cleanup any subscriptions
		m_client.setServer(host, port);
		return m_client.connect("TbDev", access_token, nullptr);
	}

	// Disconnects from ThingsBoard. Returns true on success.
	inline void disconnect() {
		m_client.disconnect();
	}

	// Returns true if connected, false otherwise.
	inline bool connected() {
		return m_client.connected();
	}

	// Executes an event loop for PubSub client.
	inline void loop() {
		m_client.loop();
	}

	//----------------------------------------------------------------------------
	// Telemetry API

	// Sends telemetry data to the ThingsBoard, returns true on success.
	template<typename T> bool sendTelemetry(const char* key, const T& value) {
		return sendKeyval(key, value);
	}

	inline bool sendTelemetry(const char* attrName, const String& value) {
		return sendTelemetry(attrName, value.c_str());
	}

	// Sends aggregated telemetry to the ThingsBoard.
	inline bool sendTelemetry(const Telemetry* data, size_t data_count) {
		return sendDataArray(data, data_count);
	}

	// Sends custom JSON telemetry string to the ThingsBoard.
	inline bool sendTelemetryJson(const char* json) {
		return m_client.publish("v1/devices/me/telemetry", json);
	}

	//----------------------------------------------------------------------------
	// Attribute API

	// Sends attribute with given name and value.
	template<typename T> bool sendAttribute(const char* attrName, const T& value) {
		return sendKeyval(attrName, value, false);
	}

	inline bool sendAttribute(const char* attrName, const String& value) {
		return sendAttribute(attrName, value.c_str());
	}

	// Sends aggregated attributes to the ThingsBoard.
	inline bool sendAttributes(const Attribute* data, size_t data_count) {
		return sendDataArray(data, data_count, false);
	}

	// Sends custom JSON with attributes to the ThingsBoard.
	inline bool sendAttributeJSON(const char* json) {
		return m_client.publish("v1/devices/me/attributes", json);
	}

	//----------------------------------------------------------------------------
	// Server-side RPC API

	// Subscribes multiple RPC callbacks with given size
	bool RPC_Subscribe(const std::vector<RPC_Callback>& callbacks) {
		if (m_subscribedInstance)
			return false;

		if (!m_client.subscribe("v1/devices/me/rpc/request/+"))
			return false;

		m_subscribedInstance = true;
		m_rpcCallbacks.assign(callbacks.begin(), callbacks.end());

		m_client.setCallback([&](char* topic, uint8_t* payload, uint32_t length) {
			if (m_subscribedInstance)
				process_message(topic, payload, length);
			});

		return true;
	}

	inline bool RPC_Unsubscribe() {
		m_subscribedInstance = false;
		return m_client.unsubscribe("v1/devices/me/rpc/request/+");
	}

	inline bool RPC_Subscribed() {
		return m_subscribedInstance;
	}

private:
	// Sends single key-value in a generic way.
	template<typename T>
	bool sendKeyval(const char* key, T value, bool telemetry = true) {
		Telemetry t(key, value);
		StaticJsonDocument<JSON_OBJECT_SIZE(1)>jsonBuffer;
		JsonVariant object = jsonBuffer.template to<JsonVariant>();

		if (!t.serializeKeyval(object)) {
			Logger::log("unable to serialize data");
			return false;
		}

		if (measureJson(jsonBuffer) > PayloadSize - 1) {
			Logger::log("too small buffer for JSON data");
			return false;
		}

		char payload[PayloadSize];
		serializeJson(object, payload, sizeof(payload));
		return telemetry ? sendTelemetryJson(payload) : sendAttributeJSON(payload);
	}

	// Processes RPC message
	void process_message(char* topic, uint8_t* payload, uint32_t length) {
		StaticJsonDocument<JSON_OBJECT_SIZE(MaxFieldsAmt)> jsonBuffer;
		DeserializationError error = deserializeJson(jsonBuffer, payload, length);

		if (error) {
			Logger::log("unable to de-serialize RPC");
			return;
		}

		const JsonObject& data = jsonBuffer.template as<JsonObject>();
		const char* methodName = data["method"];

		if (methodName) {
			Logger::log("received RPC:");
			Logger::log(methodName);
		}
		else {
			Logger::log("RPC method is nullptr");
			return;
		}

		RPC_Response r;
		for (const auto& m_rpcCallback : m_rpcCallbacks) {
			if (m_rpcCallback.m_cb && !strcmp(m_rpcCallback.m_name, methodName)) {
				Logger::log("calling RPC:");
				Logger::log(methodName);

				// Do not inform client, if parameter field is missing for some reason
				if (!data.containsKey("params"))
					Logger::log("no parameters passed with RPC, passing nullptr JSON");

				// Getting non-existing field from JSON should automatically set JSONVariant to nullptr
				r = m_rpcCallback.m_cb(data["params"]);
				break;
			}
		}

		// Fill in response
		char payloadr[PayloadSize] = { 0 };
		StaticJsonDocument<JSON_OBJECT_SIZE(1)> respBuffer;
		JsonVariant resp_obj = respBuffer.template to<JsonVariant>();

		if (!r.serializeKeyval(resp_obj)) {
			Logger::log("unable to serialize data");
			return;
		}

		if (measureJson(respBuffer) > PayloadSize - 1) {
			Logger::log("too small buffer for JSON data");
			return;
		}

		serializeJson(resp_obj, payloadr, sizeof(payloadr));
		String responseTopic = String(topic);
		responseTopic.replace("request", "response");
		Logger::log("response:");
		Logger::log(payloadr);
		m_client.publish(responseTopic.c_str(), payloadr);
	}

	// Sends array of attributes or telemetry to ThingsBoard
	bool sendDataArray(const Telemetry* data, size_t data_count, bool telemetry = true) {
		if (MaxFieldsAmt < data_count) {
			Logger::log("too much JSON fields passed");
			return false;
		}

		StaticJsonDocument<JSON_OBJECT_SIZE(MaxFieldsAmt)> jsonBuffer;
		JsonVariant object = jsonBuffer.template to<JsonVariant>();

		for (size_t i = 0; i < data_count; ++i) {
			if (!data[i].serializeKeyval(object)) {
				Logger::log("unable to serialize data");
				return false;
			}
		}

		if (measureJson(jsonBuffer) > PayloadSize - 1) {
			Logger::log("too small buffer for JSON data");
			return false;
		}

		char payload[PayloadSize];
		serializeJson(object, payload, sizeof(payload));
		return telemetry ? sendTelemetryJson(payload) : sendAttributeJSON(payload);
	}

	PubSubClient m_client;              		// PubSub MQTT client instance.
	std::vector<RPC_Callback> m_rpcCallbacks;   // RPC callbacks array	
	bool m_subscribedInstance;					// Are we subscribed to RPC?
};

#ifndef ESP8266
// ThingsBoard HTTP client class
template <size_t PayloadSize, size_t MaxFieldsAmt, typename Logger>
class ThingsBoardHttpSized
{
public:
	// Initializes ThingsBoardHttpSized class with network client.
	inline ThingsBoardHttpSized(Client& client, const char* access_token,
		const char* host, uint16_t port = 80)
		:m_client(client, host, port)
		, m_host(host)
		, m_token(access_token)
		, m_port(port)
	{ }

	// Destroys ThingsBoardHttpSized class with network client.
	inline ~ThingsBoardHttpSized() { }

	//----------------------------------------------------------------------------
	// Telemetry API

	// Sends telemetry data to the ThingsBoard, returns true on success.
	template<typename T> bool sendTelemetry(const char* key, const T& value) {
		return sendKeyval(key, value);
	}

	inline bool sendTelemetry(const char* attrName, const String& value) {
		return sendTelemetry(attrName, value.c_str());
	}

	// Sends aggregated telemetry to the ThingsBoard.
	inline bool sendTelemetry(const Telemetry* data, size_t data_count) {
		return sendDataArray(data, data_count);
	}

	// Sends custom JSON telemetry string to the ThingsBoard, using HTTP.
	inline bool sendTelemetryJson(const char* json) {
		if (!json || !m_token)
			return false;

		if (!m_client.connected() && !m_client.connect(m_host, m_port)) {
			Logger::log("connect to server failed");
			return false;
		}

		bool rc = true;
		String path = String("/api/v1/") + m_token + "/telemetry";
		if (!m_client.post(path, "application/json", json) ||
			(m_client.responseStatusCode() != HTTP_SUCCESS)) {
			rc = false;
		}

		m_client.stop();
		return rc;
	}

	//----------------------------------------------------------------------------
	// Attribute API

	// Sends attribute with given name and value.
	template<typename T> bool sendAttribute(const char* attrName, const T& value) {
		return sendKeyval(attrName, value, false);
	}

	inline bool sendAttribute(const char* attrName, const String& value) {
		return sendAttribute(attrName, value.c_str());
	}

	// Sends aggregated attributes to the ThingsBoard.
	inline bool sendAttributes(const Attribute* data, size_t data_count) {
		return sendDataArray(data, data_count, false);
	}

	// Sends custom JSON with attributes to the ThingsBoard, using HTTP.
	inline bool sendAttributeJSON(const char* json) {
		if (!json || !m_token)
			return false;

		if (!m_client.connected() && !m_client.connect(m_host, m_port)) {
			Logger::log("connect to server failed");
			return false;
		}

		bool rc = true;
		String path = String("/api/v1/") + m_token + "/attributes";
		if (!m_client.post(path, "application/json", json)
			|| (m_client.responseStatusCode() != HTTP_SUCCESS)) {
			rc = false;
		}

		m_client.stop();
		return rc;
	}

private:
	// Sends array of attributes or telemetry to ThingsBoard
	bool sendDataArray(const Telemetry* data, size_t data_count, bool telemetry = true) {
		if (MaxFieldsAmt < data_count) {
			Logger::log("too much JSON fields passed");
			return false;
		}

		StaticJsonDocument<JSON_OBJECT_SIZE(MaxFieldsAmt)> jsonBuffer;
		JsonVariant object = jsonBuffer.template to<JsonVariant>();

		for (size_t i = 0; i < data_count; ++i) {
			if (!data[i].serializeKeyval(object)) {
				Logger::log("unable to serialize data");
				return false;
			}
		}

		if (measureJson(jsonBuffer) > PayloadSize - 1) {
			Logger::log("too small buffer for JSON data");
			return false;
		}

		char payload[PayloadSize];
		serializeJson(object, payload, sizeof(payload));
		return telemetry ? sendTelemetryJson(payload) : sendAttributeJSON(payload);
	}

	// Sends single key-value in a generic way.
	template<typename T>
	bool sendKeyval(const char* key, T value, bool telemetry = true) {
		Telemetry t(key, value);
		StaticJsonDocument<JSON_OBJECT_SIZE(1)> jsonBuffer;
		JsonVariant object = jsonBuffer.template to<JsonVariant>();

		if (!t.serializeKeyval(object)) {
			Logger::log("unable to serialize data");
			return false;
		}

		if (measureJson(jsonBuffer) > PayloadSize - 1) {
			Logger::log("too small buffer for JSON data");
			return false;
		}

		char payload[PayloadSize];
		serializeJson(object, payload, sizeof(payload));
		return telemetry ? sendTelemetryJson(payload) : sendAttributeJSON(payload);
	}

	HttpClient m_client;
	const char* m_host;
	uint16_t m_port;
	const char* m_token;
};

using ThingsBoardHttp = ThingsBoardHttpSized<>;

#endif // ESP8266

using ThingsBoard = ThingsBoardSized<>;

#endif // ThingsBoard_h