package com.fluxmesh.sink;

import com.fluxmesh.model.Alert;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * MongoDB alert sink (profile {@code mongo}).
 *
 * <p>Mongo suits this shape: alerts are schema-light append-only documents that
 * are always queried newest-first with optional filters, and never joined.
 * The compound index on {@code (deviceId, ts)} matches exactly that access
 * pattern.
 */
@Component
@Profile("mongo")
public class MongoAlertSink implements AlertSink {

    private static final String COLLECTION = "alerts";

    private final MongoTemplate mongo;

    public MongoAlertSink(MongoTemplate mongo) {
        this.mongo = mongo;
        mongo.indexOps(COLLECTION).ensureIndex(
                new org.springframework.data.mongodb.core.index.Index()
                        .on("deviceId", Sort.Direction.ASC)
                        .on("ts", Sort.Direction.DESC));
    }

    @Override
    public void write(Alert alert) {
        mongo.save(alert, COLLECTION);
    }

    @Override
    public List<Alert> recent(String deviceId, String severity, int limit) {
        Query query = new Query();
        if (deviceId != null && !deviceId.isBlank()) {
            query.addCriteria(Criteria.where("deviceId").is(deviceId));
        }
        if (severity != null && !severity.isBlank()) {
            query.addCriteria(Criteria.where("severity").is(severity.toUpperCase()));
        }
        query.with(Sort.by(Sort.Direction.DESC, "ts")).limit(limit);
        return mongo.find(query, Alert.class, COLLECTION);
    }

    @Override
    public long count() {
        return mongo.getCollection(COLLECTION).countDocuments();
    }

    @Override
    public String name() {
        return "mongo";
    }
}
