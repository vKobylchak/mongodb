package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import mflix.api.models.Comment;
import mflix.api.models.Critic;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


@Component
public class CommentDao extends AbstractMFlixDao {

    public static String COMMENT_COLLECTION = "comments";

    private MongoCollection<Comment> commentCollection;
    private MongoCollection<Document> documentCommentCollection;

    private CodecRegistry pojoCodecRegistry;

    private final Logger log;

    @Autowired
    public CommentDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        log = LoggerFactory.getLogger(this.getClass());
        this.db = this.mongoClient.getDatabase(MFLIX_DATABASE);
        this.pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        this.commentCollection =
                db.getCollection(COMMENT_COLLECTION, Comment.class).withCodecRegistry(pojoCodecRegistry);
        this.documentCommentCollection = db.getCollection(COMMENT_COLLECTION);
    }

    /**
     * Returns a Comment object that matches the provided id string.
     *
     * @param id - comment identifier
     * @return Comment object corresponding to the identifier value
     */
    public Comment getComment(String id) {
        return commentCollection.find(new Document("_id", new ObjectId(id))).first();
    }

    /**
     * Adds a new Comment to the collection. The equivalent instruction in the mongo shell would be:
     *
     * <p>db.comments.insertOne({comment})
     *
     * <p>
     *
     * @param comment - Comment object.
     * @throw IncorrectDaoOperation if the insert fails, otherwise
     * returns the resulting Comment object.
     */
    public Comment addComment(Comment comment) {
        if (null == comment.getId()) throw new IncorrectDaoOperation("FAKE ID");
//        Comment actualComment = commentCollection.find(new Document("_id", new ObjectId(comment.getId()))).first();
//        try {
//            if (actualComment == null) throw new IncorrectDaoOperation("Given a wrong commentId.");
//        } catch (IncorrectDaoOperation e) {
//            e.printStackTrace();
//            return null;
//        }
        commentCollection.insertOne(comment);
        // TODO> Ticket - Update User reviews: implement the functionality that enables adding a new
        // comment.
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.

//        return actualComment;

        return commentCollection.find(new Document("_id", new ObjectId(comment.getId()))).first();


//    return commentCollection.find(new Document("_id", new ObjectId(comment.getId()))).first();
//    return null;
    }

    /**
     * Updates the comment text matching commentId and user email. This method would be equivalent to
     * running the following mongo shell command:
     *
     * <p>db.comments.update({_id: commentId}, {$set: { "text": text, date: ISODate() }})
     *
     * <p>
     *
     * @param commentId - comment id string value.
     * @param text      - comment text to be updated.
     * @param email     - user email.
     * @return true if successfully updates the comment text.
     */
    public boolean updateComment(String commentId, String text, String email) {
//      Bson match = eq("_id",
//              new ObjectId(commentId));
        Bson match = and(eq("_id",
                new ObjectId(commentId)), eq("email", email));
//      Bson set = set("text", text);
        Bson set = and(set("text", text), set("date", new Date()));
        Comment comment = commentCollection.find(match).first();
        if (null == comment) return false;

        commentCollection.updateOne(match, set);
        // TODO> Ticket - Update User reviews: implement the functionality that enables updating an
        // user own comments
        // TODO> Ticket - Handling Errors: Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        return true;
    }

    /**
     * Deletes comment that matches user email and commentId.
     *
     * @param commentId - commentId string value.
     * @param email     - user email value.
     * @return true if successful deletes the comment.
     */
    public boolean deleteComment(String commentId, String email) {
        Bson match = and(eq("_id",
                new ObjectId(commentId)), eq("email", email));
        Comment comment = commentCollection.find(match).first();
        if (null == comment) return false;
        commentCollection.deleteOne(match);

        // TODO> Ticket Delete Comments - Implement the method that enables the deletion of a user
        // comment
        // TIP: make sure to match only users that own the given commentId
        // TODO> Ticket Handling Errors - Implement a try catch block to
        // handle a potential write exception when given a wrong commentId.
        return true;
    }

    /**
     * Ticket: User Report - produce a list of users that comment the most in the website. Query the
     * `comments` collection and group the users by number of comments. The list is limited to up most
     * 20 commenter.
     *
     * @return List {@link Critic} objects.
     */
    public List<Critic> mostActiveCommenters() {
        List<Critic> mostActive = new ArrayList<>();
        List<Document> mostActiveDocument = new ArrayList<>();

        Bson match = eq("email", "andrea_le@fakegmail.com");
        long count = commentCollection.countDocuments(match);
        System.out.println(count);

//        List<Bson> bsonComments = Arrays.asList(group("$email", sum("comments", 1L)), sort(descending("comments")), limit(20));

        List<Bson> bsonComments = Arrays.asList(group("$email", first("name", "$name"), first("email", "$email"), first("movie_id", "$movie_id"), first("text", "$text"), first("date", "$date"), sum("comments", 1L)), sort(descending("comments")), limit(20));
        // // TODO> Ticket: User Report - execute a command that returns the
        // // list of 20 users, group by number of comments. Don't forget,
        // // this report is expected to be produced with an high durability
        // // guarantee for the returned documents. Once a commenter is in the
        // // top 20 of users, they become a Critic, so mostActive is composed of
        // // Critic objects.


        AggregateIterable<Document> aggregate = documentCommentCollection.aggregate(bsonComments);
//        AggregateIterable<Comment> aggregate1 = commentCollection.aggregate(bsonComments);

        aggregate.iterator().forEachRemaining(mostActiveDocument::add);

        mostActiveDocument.forEach(document -> {
            Critic critic = new Critic();
            String email = document.getString("email");
            long commentsLong = (Long) document.get("comments");
            int numComments = (int)commentsLong;
            critic.setId(email);
            critic.setNumComments(numComments);
            mostActive.add(critic);
        });

        return mostActive;
    }
}