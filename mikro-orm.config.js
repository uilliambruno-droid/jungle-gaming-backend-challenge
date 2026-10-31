import baseConfig from './dist/infrastructure/mikro-orm.config.js';
export default {
  ...baseConfig,
  entities: ['./dist/infrastructure/persistence/entities.js'],
  migrations: { path: './dist/infrastructure/persistence/migrations', pathTs: './dist/infrastructure/persistence/migrations', glob: 'Migration*.js' }
};
