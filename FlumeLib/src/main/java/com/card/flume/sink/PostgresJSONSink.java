package com.card.flume.sink;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import org.json.JSONArray;
import org.postgresql.Driver;

public class PostgresJSONSink extends AbstractSink implements Configurable {
	private String table = null;
	private String user = null;
	private String password = null;
	private String url = null;
	private Charset encoding = null;
	private Connection client = null;
	private CounterGroup counterGroup = new CounterGroup();
	private PreparedStatement insert = null;
	private String sqlStatement = null;

	private int batchSize = 0;

	private static final Logger LOG = LoggerFactory
			.getLogger(PostgresJSONSink.class);

	@Override
	public void configure(Context context) {
		try {
			// Load JDBC driver for Postgres
                        Driver driver = new Driver();
			LOG.info("Loaded postgres JDBC driver");
		} catch (Exception e) {
			throw new FlumeException("Postgres JDBC driver not on classpath");
		}

		// Get all configuration variables
		String hostname = context.getString("hostname", "localhost");
		int port = context.getInteger("port", 5432);
		String database = context.getString("database");
		table = context.getString("table", "flume");
		user = context.getString("user", null);
		password = context.getString("password", null);
		batchSize = context.getInteger("batch.size", 100);
		encoding = Charset.forName(context.getString("encoding", "UTF8"));

		// Log the properties
		LOG.info("Properties: {}", context.getParameters());

		// Create the connect string and SQL statement
		url = "jdbc:postgresql://" + hostname + ":" + port + "/" + database;
	}

	@Override
	public Status process() throws EventDeliveryException {
		// Get the channel and transaction

		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		try {
			// Start the transaction and get all events
			transaction.begin();

			List<Event> batch = new ArrayList<Event>();

			for (int i = 0; i < batchSize; ++i) {
				Event event = channel.take();

				if (event == null) {
					counterGroup.incrementAndGet("batch.underflow");
					break;
				}
				batch.add(event);
			}

			if (batch.isEmpty()) {
				counterGroup.incrementAndGet("batch.empty");
				status = Status.BACKOFF;
			} else {
				// Verify we have a connection
			    verifyConnection();

                // Ensure we have a prepared statement.
                String first = new String(batch.get(0).getBody(), encoding);
                JSONObject jsonSample = new JSONObject(first);
                JSONArray keys = jsonSample.names();
                insert = prepareSQL(keys, false);

				// For each event in the batch
				for (Event e : batch) {
					String line = new String(e.getBody(), encoding);
                    JSONObject json = new JSONObject(line);

					// Re-prepare if we don't have the right number columns.
					if (json.names().length() != keys.length()) {
                      keys = json.names();
                      insert = prepareSQL(keys, true);
                    }

                    // Iterate over the members of the JSON object.
                    int index = 1;
                    for (Object k: keys) {
                        String keyName = k.toString();
                        Object value = json.get(keyName);
                        switch (value.getClass().getName()) {
                            case "java.lang.Integer":
                                insert.setInt(index++, json.getInt(keyName));
                                break;
                            case "java.lang.String":
                                insert.setString(index++, json.getString(keyName));
                                break;
                            case "java.lang.Double":
                                insert.setDouble(index++, json.getDouble(keyName));
                                break;
                            case "java.lang.Long":
                                insert.setLong(index++, json.getLong(keyName));
                                break;
                            case "java.lang.Boolean":
                                insert.setBoolean(index++, json.getBoolean(keyName));
                                break;
                        }

        			    insert.executeUpdate();
		       	        counterGroup.incrementAndGet("statements.commit");
		            }
				}
				// Commit to Postgres
				client.commit();

				// Increment that this was a successful batch
				counterGroup.incrementAndGet("batch.success");
			}

			// Commit the transmission
			transaction.commit();
			transaction.close();

			LOG.debug("Counters: {}", counterGroup);
		} catch (Exception e) {
			transaction.rollback();
			transaction.close();
			throw new EventDeliveryException(e);
		}
		return status;
	}

	@Override
	public synchronized void start() {
		try {
			openConnection();
		} catch (SQLException e) {
			throw new FlumeException(e);
		}

		super.start();
		LOG.info("Postgres Sink started");
	}

	@Override
	public synchronized void stop() {
		try {
			destroyConnection();
		} catch (SQLException e) {
			throw new FlumeException(e);
		}

		super.stop();
		LOG.info("Postgres Sink stopped");
	}

    /**
     * Builds the SQL string for the preared Statement.
     */
    private PreparedStatement prepareSQL(JSONArray keys, boolean forceRebuild) throws SQLException {
        if ((sqlStatement == null) || forceRebuild) {
            sqlStatement = "INSERT INTO " + table + " (";

            int numColumns = 0;
            for (Object k : keys) {
              sqlStatement += k.toString() + ", ";
              numColumns++;
            }

            sqlStatement += ") VALUES (";

            for (int i = 0; i < numColumns; ++i) {
                sqlStatement += "?,";
            }

            sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1);
            sqlStatement += ");";

            LOG.info("Statement: {}", sqlStatement);
            insert = client.prepareStatement(sqlStatement);
        }
        return insert;
    }

	/**
	 * Verifies that a connection is valid and will open one if needed.
	 *
	 * @throws SQLException
	 */
	private void verifyConnection() throws SQLException {
		if (client == null) {
			openConnection();
		} else if (client.isClosed()) {
			destroyConnection();
			openConnection();
		}
	}

	/**
	 * Opens the given connection and creates the prepared statement
	 *
	 * @throws SQLException
	 */
	private void openConnection() throws SQLException {
		Properties props = new Properties();

		if (user != null && password != null) {
			props.setProperty("user", user);
			props.setProperty("password", password);
		} else if (user != null ^ password != null) {
			LOG.warn("User or password is set without the other. Continuing with no login auth");
		}

		client = DriverManager.getConnection(url, props);
		client.setAutoCommit(false);
		LOG.info("Opened client connection and prepared insert statement");
	}

	/**
	 * Destroys the current JDBC connection, if any.
	 *
	 * @throws SQLException
	 */
	private void destroyConnection() throws SQLException {
		if (client != null) {
			client.close();
			client = null;
			LOG.info("Closed client connection");
			LOG.info("Counters: {}", counterGroup);
		}
	}
}
/* vim: set shiftwidth=4 tabstop=4 */
