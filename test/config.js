var env = process.env;
// module.exports = {
// "host": env.DB_PORT_5432_TCP_ADDR || "127.0.0.1",
// "port": env.DB_PORT_5432_TCP_PORT || 65432, // 5432,
// "database": env.DB_ENV_POSTGRES_DB || 'mosaic', // 'postgres',
// "user": env.DB_ENV_POSTGRES_USER || 'docker', // 'postgres',
// "pass": env.DB_ENV_POSTGRES_PASSWORD || 'docker', // 'postgres'
// }

module.exports = {
    testDbName : 'test_db_one',
    db : {
        "host" : "127.0.0.1",
        "port" : 65432, // 5432,
        "database" : "mosaic", // 'postgres',
        "user" : "docker", //  'postgres',
        "password" : "docker", // 'postgres'
    }
}