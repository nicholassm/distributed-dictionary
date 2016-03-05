package systems.speedy.distributed.dictionary;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZooKeeperBackend implements Closeable {
	private final static String NAMESPACE = "systems.speedy.distributed.dictionary";
	private final CuratorFramework client;

	private ZooKeeperBackend(String connectionString, RetryPolicy retryPolicy) throws InterruptedException {
		client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);

		client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
			@Override public void stateChanged(CuratorFramework client, ConnectionState newState) {
				// TODO: Use this information appropriately.
				System.err.println("New Connection State: "+newState);
			}
		});

		client.start();
		client.blockUntilConnected();
	}

	public DistributedByteDictionary newByteDictionary(String name) {
		return new ByteDictionaryImplementation(name);
	}

	@Override public void close() throws IOException {
		client.close();
	}

	public static class Builder {
		private String connectionString;
		private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

		public Builder withConnectionString(String connectionString) {
			this.connectionString = connectionString;
			return this;
		}

		public Builder withRetryPolicy(RetryPolicy retryPolicy) {
			this.retryPolicy = Objects.requireNonNull(retryPolicy, "RetryPolicy cannot be null.");
			return this;
		}

		public ZooKeeperBackend build() throws InterruptedException {
			Objects.requireNonNull(connectionString, "connectionString cannot be null.");
			return new ZooKeeperBackend(connectionString, retryPolicy);
		}
	}

	private class ByteDictionaryImplementation implements DistributedByteDictionary {
		private final String name;
		private final InterProcessReadWriteLock readWriteLock;

		// TODO: Pick efficient hashmaps for two way lookup.
		private final Map<String, Byte> stringsToBytes;
		private final Map<Byte, String> bytesToStrings;

		private ByteDictionaryImplementation(String name) {
			this.name = name;
			this.stringsToBytes = new HashMap<>();
			this.bytesToStrings = new HashMap<>();
			this.readWriteLock  = new InterProcessReadWriteLock(client, NAMESPACE+"/"+"lock");
		}

		@Override public byte lookup(String s) {
			return stringsToBytes.computeIfAbsent(s, this::lookupOrAssignByteInZookeeper);
		}

		@Override public String lookup(byte code) {
			return bytesToStrings.computeIfAbsent(code, this::lookupStringInZookeeper);
		}

		private byte lookupOrAssignByteInZookeeper(String unknownString) {
			// TODO: Store newString in ZooKeeper.
			return 0;
		}

		private String lookupStringInZookeeper(byte code) {
			// TODO: Lookup code in ZooKeeper if absent.
			return "";
		}
	}
}
