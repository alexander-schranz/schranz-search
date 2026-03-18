<?php

declare(strict_types=1);

/*
 * This file is part of the CMS-IG SEAL project.
 *
 * (c) Alexander Schranz <alexander@sulu.io>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace CmsIg\Seal\Adapter\MongoDB;

use CmsIg\Seal\Adapter\SchemaManagerInterface;
use CmsIg\Seal\Schema\Field;
use CmsIg\Seal\Schema\Index;
use CmsIg\Seal\Task\AsyncTask;
use CmsIg\Seal\Task\TaskInterface;

final class MongoDBSchemaManager implements SchemaManagerInterface
{
    public function __construct(
        private readonly ClientWrapper $client,
    ) {
    }

    public function existIndex(Index $index): bool
    {
        $collectionNames = [...$this->client->getDatabase()->listCollectionNames()];

        return \in_array($index->name, $collectionNames, true);
    }

    public function dropIndex(Index $index, array $options = []): TaskInterface|null
    {
        $database = $this->client->getDatabase();
        $database->dropCollection($index->name);

        if (!($options['return_slow_promise_result'] ?? false)) {
            return null;
        }

        return new AsyncTask(function () use ($index): null {
            $this->waitForSearchIndexState($index, false);

            return null;
        });
    }

    public function createIndex(Index $index, array $options = []): TaskInterface|null
    {
        $database = $this->client->getDatabase();
        $indexConfig = $this->createIndexConfig($index->fields);

        $validator = [
            '$jsonSchema' => [
                'bsonType' => 'object',
                'properties' => $indexConfig['properties'],
                'additionalProperties' => true,
            ],
        ];

        $database->createCollection($index->name, [
            'validator' => $validator,
        ]);

        $collection = $database->getCollection($index->name);
        foreach ($indexConfig['indexes'] as $indexName => $indexType) {
            $collection->createIndex([
                $indexName => $indexType,
            ]);
        }

        $searchDefinition = [
            'mappings' => [
                'dynamic' => false,
                'fields' => $indexConfig['mappingFields'],
            ],
        ];

        $collection->createSearchIndex(
            $searchDefinition,
            ['name' => $index->name . '-search-index'],
        );

        if (!($options['return_slow_promise_result'] ?? false)) {
            return null;
        }

        return new AsyncTask(function () use ($index): null {
            $this->waitForSearchIndexState($index, true);

            return null;
        });
    }

    private function waitForSearchIndexState(Index $index, bool $shouldExist): void
    {
        $collection = $this->client->getDatabase()->getCollection($index->name);
        $searchIndexName = $index->name . '-search-index';

        for ($attempt = 0; $attempt < 600; ++$attempt) {
            if (!$this->existIndex($index)) {
                if (!$shouldExist) {
                    return;
                }

                \usleep(100_000);
                continue;
            }

            $foundSearchIndex = false;
            try {
                foreach ($collection->listSearchIndexes([
                    'typeMap' => [
                        'root' => 'array',
                        'document' => 'array',
                    ],
                ]) as $searchIndex) {
                    if (!\is_array($searchIndex)) {
                        continue;
                    }

                    if (($searchIndex['name'] ?? null) !== $searchIndexName) {
                        continue;
                    }

                    $foundSearchIndex = true;
                    $queryable = true === ($searchIndex['queryable'] ?? null) || 'READY' === ($searchIndex['status'] ?? null);

                    if ($shouldExist && $queryable) {
                        return;
                    }
                }
            } catch (\Throwable) {
                if (!$shouldExist) {
                    return;
                }
            }

            if (!$shouldExist && !$foundSearchIndex) {
                return;
            }

            \usleep(100_000);
        }

        throw new \RuntimeException(\sprintf(
            'Search index "%s" in "%s" did not reach expected state (%s).',
            $searchIndexName,
            $index->name,
            $shouldExist ? 'queryable' : 'removed',
        ));
    }

    /**
     * @param array<string, Field\AbstractField> $fields
     *
     * @return array{
     *     properties: array<string, mixed>,
     *     mappingFields: array<string, mixed>,
     *     indexes: array<string, string>,
     * }
     */
    private function createIndexConfig(array $fields, bool $collectGeoIndexFields = true): array
    {
        /** @var array<string, mixed> $properties */
        $properties = [];
        /** @var array<string, mixed> $mappingFields */
        $mappingFields = [];
        /** @var array<string, string> $indexes */
        $indexes = [];

        foreach ($fields as $name => $field) {
            match (true) {
                $field instanceof Field\IdentifierField => $properties[$name] = [
                    'bsonType' => ['string', 'null'],
                ],
                $field instanceof Field\TextField => $properties[$name] = $field->multiple ? [
                    'bsonType' => ['array', 'null'],
                    'items' => [
                        'bsonType' => ['string', 'null'],
                    ],
                ] : [
                    'bsonType' => ['string', 'null'],
                ],
                $field instanceof Field\BooleanField => $properties[$name] = $field->multiple ? [
                    'bsonType' => ['array', 'null'],
                    'items' => [
                        'bsonType' => ['bool', 'null'],
                    ],
                ] : [
                    'bsonType' => ['bool', 'null'],
                ],
                $field instanceof Field\DateTimeField => $properties[$name] = $field->multiple ? [
                    'bsonType' => ['array', 'null'],
                    'items' => [
                        'bsonType' => ['string', 'null'],
                    ],
                ] : [
                    'bsonType' => ['string', 'null'],
                ],
                $field instanceof Field\IntegerField => $properties[$name] = $field->multiple ? [
                    'bsonType' => ['array', 'null'],
                    'items' => [
                        'bsonType' => ['int', 'null'],
                    ],
                ] : [
                    'bsonType' => ['int', 'null'],
                ],
                $field instanceof Field\FloatField => $properties[$name] = $field->multiple ? [
                    'bsonType' => ['array', 'null'],
                    'items' => [
                        'bsonType' => ['double', 'null'],
                    ],
                ] : [
                    'bsonType' => ['double', 'null'],
                ],
                $field instanceof Field\GeoPointField => $properties[$name] = [
                    'bsonType' => ['object', 'null'],
                    'properties' => [
                        'type' => ['bsonType' => 'string'],
                        'coordinates' => ['bsonType' => 'array'],
                    ],
                    'additionalProperties' => true,
                ],
                $field instanceof Field\ObjectField => $properties[$name] = $this->createObjectProperties($field),
                $field instanceof Field\JsonObjectField => $properties[$name] = [
                    'bsonType' => ['string', 'null'],
                ],
                $field instanceof Field\TypedField => $properties[$name] = $this->createTypedProperties($field),
                default => $properties[$name] = ['bsonType' => ['null']],
            };

            if (($field instanceof Field\TextField || $field instanceof Field\IdentifierField) && $field->searchable) {
                $mappingFields[$name] = ['type' => 'string'];
            }

            if ($field instanceof Field\ObjectField) {
                $objectConfig = $this->createIndexConfig($field->fields, false);
                if ([] !== $objectConfig['mappingFields']) {
                    $mappingFields[$name] = [
                        'type' => 'document',
                        'fields' => $objectConfig['mappingFields'],
                    ];
                }
            }

            if ($field instanceof Field\TypedField) {
                $typedMappingFields = [];

                foreach ($field->types as $type => $typedFields) {
                    $typedConfig = $this->createIndexConfig($typedFields, false);
                    if ([] !== $typedConfig['mappingFields']) {
                        $typedMappingFields[$type] = [
                            'type' => 'document',
                            'fields' => $typedConfig['mappingFields'],
                        ];
                    }
                }

                if ([] !== $typedMappingFields) {
                    $mappingFields[$name] = [
                        'type' => 'document',
                        'fields' => $typedMappingFields,
                    ];
                }
            }

            if ($collectGeoIndexFields && $field instanceof Field\GeoPointField) {
                $indexes[$name] = '2dsphere';
            }
        }

        return [
            'properties' => $properties,
            'mappingFields' => $mappingFields,
            'indexes' => $indexes,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function createObjectProperties(Field\ObjectField $field): array
    {
        $objectConfig = $this->createIndexConfig($field->fields, false);

        return [
            'bsonType' => ['object', 'array', 'null'],
            'properties' => $objectConfig['properties'],
            'additionalProperties' => true,
        ];
    }

    /**
     * @return array<string, mixed>
     */
    private function createTypedProperties(Field\TypedField $field): array
    {
        $typedProperties = [];

        foreach ($field->types as $type => $fields) {
            $typedConfig = $this->createIndexConfig($fields, false);
            $typedProperties[$type] = [
                'bsonType' => ['object', 'array', 'null'],
                'properties' => $typedConfig['properties'],
                'additionalProperties' => true,
            ];
        }

        return [
            'bsonType' => ['object', 'array', 'null'],
            'properties' => $typedProperties,
            'additionalProperties' => true,
        ];
    }
}
