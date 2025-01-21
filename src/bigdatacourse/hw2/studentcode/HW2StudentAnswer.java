package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import bigdatacourse.hw2.HW2API;

public class HW2StudentAnswer implements HW2API {
	
	// general consts
	private static final String		NOT_AVAILABLE_VALUE 	=		"na";
	private static final String		TABLE_ITEM_VIEW			=		"item_view";
	private static final String		TABLE_USER_REVIEWS_VIEW	=		"user_reviews_view";
	private static final String		TABLE_ITEM_REVIEWS_VIEW	=		"item_reviews_view";
	private static final int	 	MAX_THREADS				=		250;

	// CQL stuff
	private static final String 		CQL_ITEM_VIEW_CREATE_TABLE =
			"CREATE TABLE " + TABLE_ITEM_VIEW 							+
				"("		 												+
					"asin text," 										+
					"title text," 										+
					"imageURL text," 									+
					"categories set<text>," 							+
					"description text,"									+
					"PRIMARY KEY (asin)"								+
				")";
	private static final String 		CQL_USER_REVIEWS_VIEW_CREATE_TABLE =
			"CREATE TABLE " + TABLE_USER_REVIEWS_VIEW 					+
				"("		 												+
					"reviewerID text," 									+
					"time timestamp," 									+
					"asin text," 										+
					"reviewName text," 									+
					"rating int," 										+
					"summary text," 									+
					"reviewText text," 									+
					"PRIMARY KEY ((reviewerID), time, asin)"			+
				")"														+
			"WITH CLUSTERING ORDER BY (time DESC, asin DESC)";
	private static final String 		CQL_ITEM_REVIEWS_VIEW_CREATE_TABLE =
			"CREATE TABLE " + TABLE_ITEM_REVIEWS_VIEW 					+
				"("		 												+
					"asin text," 										+
					"time timestamp," 									+
					"reviewerID text," 									+
					"reviewerName text," 								+
					"rating int," 										+
					"summary text," 									+
					"reviewText text," 									+
					"PRIMARY KEY ((asin), time, reviewerID)"	+
				")"														+
			"WITH CLUSTERING ORDER BY (time DESC, reviewerID DESC)";

		// Item queries
	private static final String		CQL_ITEM_VIEW_INSERT =
			"INSERT INTO " + TABLE_ITEM_VIEW + "(asin, title, imageURL, categories, description) VALUES(?, ?, ?, ?, ?)";
	private static final String		CQL_ITEM_VIEW_SELECT =
			"SELECT * FROM " + TABLE_ITEM_VIEW + " WHERE asin = ?";

		// User reviews queries
	private static final String		CQL_USER_REVIEWS_VIEW_INSERT =
			"INSERT INTO " + TABLE_USER_REVIEWS_VIEW + "(reviewerID, time, asin, reviewName, rating, summary, reviewText) VALUES(?, ?, ?, ?, ?, ?, ?)";
	private static final String		CQL_USER_REVIEWS_VIEW_SELECT =
			"SELECT * FROM " + TABLE_USER_REVIEWS_VIEW + " WHERE reviewerID = ?";

		// Item reviews queries
	private static final String		CQL_ITEM_REVIEWS_VIEW_INSERT =
			"INSERT INTO " + TABLE_ITEM_REVIEWS_VIEW + "(asin, time, reviewerID, reviewerName, rating, summary, reviewText) VALUES(?, ?, ?, ?, ?, ?, ?)";
	private static final String		CQL_ITEM_REVIEWS_VIEW_SELECT =
			"SELECT * FROM " + TABLE_ITEM_REVIEWS_VIEW + " WHERE asin = ?";


	
	// cassandra session
	private CqlSession session;
	
	// prepared statements
	// Item prepared statements
	private PreparedStatement insertItemView;
	private PreparedStatement selectItemView;

	// User reviews prepared statements
	private PreparedStatement insertUserReviewsView;
	private PreparedStatement selectUserReviewsView;

	// Item reviews prepared statements
	private PreparedStatement insertItemReviewsView;
	private PreparedStatement selectItemReviewsView;
	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			System.out.println("ERROR - cassandra is already connected");
			return;
		}
		
		System.out.println("Initializing connection to Cassandra...");
		
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		
		System.out.println("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			System.out.println("Cassandra connection is already closed");
			return;
		}
		
		System.out.println("Closing Cassandra connection...");
		session.close();
		System.out.println("Closing Cassandra connection... Done");
	}

	
	
	@Override
	public void createTables() {

		session.execute(CQL_ITEM_VIEW_CREATE_TABLE);
		session.execute(CQL_USER_REVIEWS_VIEW_CREATE_TABLE);
		session.execute(CQL_ITEM_REVIEWS_VIEW_CREATE_TABLE);

	}

	@Override
	public void initialize() {

		// Item prepared statements
		this.insertItemView = session.prepare(CQL_ITEM_VIEW_INSERT);
		this.selectItemView = session.prepare(CQL_ITEM_VIEW_SELECT);

		// User reviews prepared statements
		this.insertUserReviewsView = session.prepare(CQL_USER_REVIEWS_VIEW_INSERT);
		this.selectUserReviewsView = session.prepare(CQL_USER_REVIEWS_VIEW_SELECT);

		// Item reviews prepared statements
		this.insertItemReviewsView = session.prepare(CQL_ITEM_REVIEWS_VIEW_INSERT);
		this.selectItemReviewsView = session.prepare(CQL_ITEM_REVIEWS_VIEW_SELECT);
	}

	@Override
	public void loadItems(String pathItemsFile) throws Exception {
		BufferedReader br = getBufferedReader(pathItemsFile);
		String line;

		ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);

		while ((line = br.readLine()) != null) {
			final String currentLine = line;
			executor.execute(new Runnable() {
				@Override
				public void run() {
					JSONObject itemJSON = getTableJSONObject(currentLine, true);

					JSONArray categoriesArray = itemJSON.getJSONArray("categories");
					Set<String> categories = new HashSet<>();
					for (int i = 0; i < categoriesArray.length(); i++) {
						categories.add(categoriesArray.getString(i));
					}
					insertItemView(
							itemJSON.getString("asin"),
							itemJSON.getString("title"),
							itemJSON.getString("imageURL"),
							categories,
							itemJSON.getString("description"));
				}
			});
		}
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		br.close();
	}

	@Override
	public void loadReviews(String pathReviewsFile) throws Exception {
		BufferedReader br = getBufferedReader(pathReviewsFile);
		String line;
		ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
		while ((line = br.readLine()) != null) {
			final String currentLine = line;
			executor.execute(new Runnable() {
				@Override
				public void run() {
					JSONObject reviewItemJSON = getTableJSONObject(currentLine, false);
					insertItemReviewsView(
							reviewItemJSON.getString("asin"),
							Instant.ofEpochSecond(reviewItemJSON.getLong("time")),
							reviewItemJSON.getString("reviewerID"),
							reviewItemJSON.getString("reviewerName"),
							reviewItemJSON.getInt("rating"),
							reviewItemJSON.getString("summary"),
							reviewItemJSON.getString("reviewText")
					);

					insertUserReviewsView(
							reviewItemJSON.getString("reviewerID"),
							Instant.ofEpochSecond(reviewItemJSON.getLong("time")),
							reviewItemJSON.getString("asin"),
							reviewItemJSON.getString("reviewerName"),
							reviewItemJSON.getInt("rating"),
							reviewItemJSON.getString("summary"),
							reviewItemJSON.getString("reviewText")
					);
				}
			});

		}
		executor.shutdown();
		executor.awaitTermination(1, TimeUnit.HOURS);
		br.close();
	}

	@Override
	public String item(String asin) {
		String item;
		Row itemSelectResult = selectItemView(asin);
		if (itemSelectResult != null) {
			TreeSet<String> categories = new TreeSet<String>(itemSelectResult.getSet("categories", String.class));
			item = formatItem(
					itemSelectResult.getString("asin"),
					itemSelectResult.getString("title"),
					itemSelectResult.getString("imageURL"),
					categories,
					itemSelectResult.getString("description")
			);
		} else {
			item = "not exists\n";
		}
		return item;
	}
	
	
	@Override
	public Iterable<String> userReviews(String reviewerID) {
		// the order of the reviews should be by the time (desc), then by the asin
		ResultSet userReviewsSelectResults = selectUserReviewsView(reviewerID);
		ArrayList<String> reviewReprs = new ArrayList<String>();
		Row userReview;
		while ((userReview = userReviewsSelectResults.one()) != null) {
			String reviewRepr = formatReview(
					userReview.getInstant("time"),
					userReview.getString("asin"),
					userReview.getString("reviewerID"),
					userReview.getString("reviewName"),
					userReview.getInt("rating"),
					userReview.getString("summary"),
					userReview.getString("reviewText")
			);
			reviewReprs.add(reviewRepr);
		}
		return reviewReprs;
	}

	@Override
	public Iterable<String> itemReviews(String asin) {
		// the order of the reviews should be by the time (desc), then by the reviewerID
		ResultSet itemReviewsSelectResults = selectItemReviewsView(asin);
		ArrayList<String> reviewReprs = new ArrayList<String>();
		Row itemReview;

		while ((itemReview = itemReviewsSelectResults.one()) != null) {
			String reviewRepr = formatReview(
					itemReview.getInstant("time"),
					itemReview.getString("asin"),
					itemReview.getString("reviewerID"),
					itemReview.getString("reviewerName"),
					itemReview.getInt("rating"),
					itemReview.getString("summary"),
					itemReview.getString("reviewText")
			);
			reviewReprs.add(reviewRepr);
		}
		return reviewReprs;
	}

	// Formatting methods, do not change!
	private String formatItem(String asin, String title, String imageUrl, Set<String> categories, String description) {
		String itemDesc = "";
		itemDesc += "asin: " + asin + "\n";
		itemDesc += "title: " + title + "\n";
		itemDesc += "image: " + imageUrl + "\n";
		itemDesc += "categories: " + categories.toString() + "\n";
		itemDesc += "description: " + description + "\n";
		return itemDesc;
	}

	private String formatReview(Instant time, String asin, String reviewerId, String reviewerName, Integer rating, String summary, String reviewText) {
		String reviewDesc = 
			"time: " + time + 
			", asin: " 	+ asin 	+
			", reviewerID: " 	+ reviewerId +
			", reviewerName: " 	+ reviewerName 	+
			", rating: " 		+ rating	+ 
			", summary: " 		+ summary +
			", reviewText: " 	+ reviewText + "\n";
		return reviewDesc;
	}

	private BufferedReader getBufferedReader(String path) {
		try {
			return new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	private JSONObject getTableJSONObject(String stringJSON, boolean isItem) {
		JSONObject withNaJSON = this.addNaToJSON(stringJSON, isItem);
		JSONObject parsedJSON = new JSONObject();
        if (isItem) {
            parsedJSON.put("asin", withNaJSON.get("asin"));
            parsedJSON.put("title", withNaJSON.get("title"));
            parsedJSON.put("imageURL", withNaJSON.get("imageURL"));
            parsedJSON.put("categories", withNaJSON.get("categories"));
            parsedJSON.put("description", withNaJSON.get("description"));
        } else {
            parsedJSON.put("reviewerID", withNaJSON.get("reviewerID"));
            parsedJSON.put("time", withNaJSON.get("time"));
            parsedJSON.put("asin", withNaJSON.get("asin"));
            parsedJSON.put("reviewerName", withNaJSON.get("reviewerName"));
            parsedJSON.put("rating", withNaJSON.get("rating"));
            parsedJSON.put("summary", withNaJSON.get("summary"));
            parsedJSON.put("reviewText", withNaJSON.get("reviewText"));
        }
		return parsedJSON;
	}

	private JSONObject addNaToJSON(String stringJSON, boolean isItem) {
		JSONObject rawJSON = new JSONObject(stringJSON);
		JSONObject withNaJSON = new JSONObject();
		if (isItem) {
			addNaToItem(rawJSON, withNaJSON);
		} else {
			addNaToReview(withNaJSON, rawJSON);
		}
		return withNaJSON;
	}

	private void addNaToReview(JSONObject withNaJSON, JSONObject rawJSON) {
		String[] reviewKeysArray = {"asin", "reviewerID", "unixReviewTime", "reviewerName", "overall", "summary", "reviewText"};
		for (String key : reviewKeysArray) {
			try {
				if (key.equals("unixReviewTime")) {
					withNaJSON.put("time", rawJSON.get(key));
				} else if (key.equals("overall")) {
					withNaJSON.put("rating", rawJSON.get(key));
				} else {
					withNaJSON.put(key, rawJSON.get(key));
				}
			}
			catch (Exception e) {
				if (key.equals("unixReviewTime")) {
					withNaJSON.put("time", NOT_AVAILABLE_VALUE);
				} else if (key.equals("overall")) {
					withNaJSON.put("rating", 0);
				} else {
					withNaJSON.put(key, NOT_AVAILABLE_VALUE);
				}
			}
		}
	}

	private void addNaToItem(JSONObject rawJSON, JSONObject withNaJSON) {
		String[] itemKeysArray = {"asin", "title", "imUrl", "categories", "description"};
		for (String key : itemKeysArray) {
			try {
				switch (key) {
					case "categories":
						JSONArray outerArray = rawJSON.getJSONArray("categories");
						JSONArray categoriesArray = outerArray.getJSONArray(0);
						withNaJSON.put("categories", categoriesArray);
						break;
					case "imUrl":
						withNaJSON.put("imageURL", rawJSON.get(key));
						break;
					default:
						withNaJSON.put(key, rawJSON.get(key));
				}
			}
			catch (Exception e) {
				if (key.equals("imUrl")) {
					withNaJSON.put("imageURL", NOT_AVAILABLE_VALUE);
				}
				else {
					withNaJSON.put(key, NOT_AVAILABLE_VALUE);
				}
			}
		}
	}

	private void insertItemView(String asin, String title, String imageURL, Set<String> categories, String description) {
		BoundStatement insertItemBstmt = insertItemView.bind()
						.setString("asin", asin)
						.setString("title", title)
						.setString("imageURL", imageURL)
						.setSet("categories", categories, String.class)
						.setString("description", description);
		session.execute(insertItemBstmt);
	}

	private Row selectItemView(String asin) {
		BoundStatement selectItemBstmt = selectItemView.bind()
				.setString(0, asin);
		return session.execute(selectItemBstmt).one();
	}

	private void insertUserReviewsView(String reviewerID, Instant time, String asin, String reviewName, int rating, String summary, String reviewText) {
		BoundStatement insertUserReviewsBstmt = insertUserReviewsView.bind()
						.setString("reviewerID", reviewerID)
						.setInstant("time", time)
						.setString("asin", asin)
						.setString("reviewName", reviewName)
						.setInt("rating", rating)
						.setString("summary", summary)
						.setString("reviewText", reviewText);
		session.execute(insertUserReviewsBstmt);
	}

	private ResultSet selectUserReviewsView(String reviewerID) {
		BoundStatement selectUserReviewsBstmt = selectUserReviewsView.bind()
				.setString(0, reviewerID);
		return session.execute(selectUserReviewsBstmt);
	}

	private void insertItemReviewsView(String asin, Instant time, String reviewerID, String reviewerName, int rating, String summary, String reviewText) {
		BoundStatement insertItemReviewsBstmt = insertItemReviewsView.bind()
						.setString("asin", asin)
						.setInstant("time", time)
						.setString("reviewerID", reviewerID)
						.setString("reviewerName", reviewerName)
						.setInt("rating", rating)
						.setString("summary", summary)
						.setString("reviewText", reviewText);
		session.execute(insertItemReviewsBstmt);
	}

	private ResultSet selectItemReviewsView(String asin) {
		BoundStatement selectItemReviewsBstmt = selectItemReviewsView.bind()
				.setString(0, asin);
		return session.execute(selectItemReviewsBstmt);
	}
}
