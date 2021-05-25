package config;

import lombok.SneakyThrows;
import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.Request;

public class Config {

    private final static String endpoint = "localhost:5002";

    @SneakyThrows
    public static NoSQLHandle getHandle() {
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint);
        configureAuth(config);
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);
        return handle;
    }

    private static void configureAuth(NoSQLHandleConfig config) {
        /* cloud simulator */
        config.setAuthorizationProvider(new AuthorizationProvider() {
            @Override
            public String getAuthorizationString(Request request) {
                return "Bearer cloudsim";
            }

            @Override
            public void close() {
            }
        });

    }

}