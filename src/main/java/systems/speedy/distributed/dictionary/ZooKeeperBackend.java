package systems.speedy.distributed.dictionary;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

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
		private final InterProcessMutex writeLock;
		private final InterProcessMutex readLock;

		private static final String LOCK_PATH  = "readWriteLock";
		private static final String ENTRY_PATH = "entry";

		// TODO: Pick efficient hashmaps for two way lookup.
		private final Map<String, Byte> stringsToBytes;
		private final Map<Byte, String> bytesToStrings;

		private ByteDictionaryImplementation(String name) {
			this.name = name;
			this.stringsToBytes = new HashMap<>();
			this.bytesToStrings = new HashMap<>();

			InterProcessReadWriteLock readWriteLock = new InterProcessReadWriteLock(client, prependNamespace(LOCK_PATH));
			this.writeLock = readWriteLock.writeLock();
			this.readLock  = readWriteLock.readLock();
		}

		@Override public byte lookup(String s) {
			return stringsToBytes.computeIfAbsent(s, this::lookupOrAssignByteInZookeeper);
		}

		@Override public String lookup(byte code) {
			return bytesToStrings.computeIfAbsent(code, this::lookupStringInZookeeper);
		}

		private String prependNamespace(String path) {
			return NAMESPACE + "/" + name + "/" + path;
		}

		private String stringFromDataNode(byte[] bytes) {
			return new String(Arrays.copyOfRange(bytes, 1, bytes.length));
		}

		private byte byteFromDataNode(byte[] bytes) {
			return bytes[0];
		}

		private byte lookupOrAssignByteInZookeeper(String unknownString) {
			try {
				try {
					writeLock.acquire();
					// TODO: Does ZooKeeper provide any guarantees related to the ordering of nodes?
					// In creation order always?
					List<String> nodes = client.getChildren().forPath(prependNamespace(ENTRY_PATH));

					byte largestSeenValue = 0;
					for (String node : nodes) {
						byte[] bytes        = client.getData().forPath(node);
						String storedString = stringFromDataNode(bytes);
						byte storedByte     = byteFromDataNode(bytes);
						if (unknownString.equals(storedString)) {
							return byteFromDataNode(bytes);
						}
						if (storedByte > largestSeenValue) largestSeenValue = storedByte;
					}

					// Not found.
					byte newByte = ++largestSeenValue;
					store(unknownString, newByte);
					return newByte;
				}
				finally {
					writeLock.release();
				}
			}
			catch (Exception e) {
				// TODO: How to handle this case?
			}
			throw new AssertionError();
		}

		private void store(String unknownString, byte newByte) throws Exception {
			byte[] unknownStringBytes = unknownString.getBytes();
			byte[] newMapping         = new byte[1 + unknownStringBytes.length];
			newMapping[0]             = newByte;
			System.arraycopy(unknownStringBytes, 0, newMapping, 1, unknownStringBytes.length);

			client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(prependNamespace(ENTRY_PATH), newMapping);
		}

		private String lookupStringInZookeeper(byte code) {
			try {
				try {
					readLock.acquire();
					List<String> nodes = client.getChildren().forPath(prependNamespace(ENTRY_PATH));

					for (String node : nodes) {
						byte[] bytes        = client.getData().forPath(node);
						byte storedByte     = byteFromDataNode(bytes);
						if (code == storedByte) {
							String storedString = stringFromDataNode(bytes);
							return storedString;
						}
					}
				}
				finally {
					readLock.release();
				}
			}
			catch (Exception e) {
				// TODO: How to handle this case?
			}
			throw new AssertionError();
		}
	}
}
