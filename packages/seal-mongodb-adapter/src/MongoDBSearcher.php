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

use CmsIg\Seal\Adapter\SearcherInterface;
use CmsIg\Seal\Marshaller\Marshaller;
use CmsIg\Seal\Schema\Field\GeoPointField;
use CmsIg\Seal\Schema\Index;
use CmsIg\Seal\Search\Condition;
use CmsIg\Seal\Search\Facet\AbstractFacet;
use CmsIg\Seal\Search\Facet\CountFacet;
use CmsIg\Seal\Search\Facet\MinMaxFacet;
use CmsIg\Seal\Search\Result;
use CmsIg\Seal\Search\Search;
use MongoDB\Driver\Exception\Exception;

final class MongoDBSearcher implements SearcherInterface
{
    private readonly Marshaller $marshaller;

    public function __construct(
        private readonly ClientWrapper $client,
    ) {
        $this->marshaller = new Marshaller(
            geoPointFieldConfig: [
                'latitude' => 'lat',
                'longitude' => 'lon',
            ],
        );
    }

    public function count(Index $index): int
    {
        try {
            return $this->client->getDatabase()
                ->getCollection($index->name)
                ->countDocuments();
        } catch (Exception) {
            return 0;
        }
    }

    public function search(Search $search): Result
    {
        // optimized single document query
        if (
            1 === \count($search->filters)
            && $search->filters[0] instanceof Condition\IdentifierCondition
            && 0 === $search->offset
            && 1 === $search->limit
        ) {
            /** @var array<string, mixed>|null $searchResult */
            $searchResult = $this->client->getDatabase()
                ->getCollection($search->index->name)
                ->findOne(
                    ['_id' => $search->filters[0]->identifier],
                    [
                        'typeMap' => [
                            'root' => 'array',
                            'document' => 'array',
                        ],
                    ],
                );

            if (null === $searchResult) {
                return new Result(
                    $this->hitsToDocuments($search->index, [], [], $search->highlightPreTag, $search->highlightPostTag),
                    0,
                );
            }

            return new Result(
                $this->hitsToDocuments($search->index, [$searchResult], [], $search->highlightPreTag, $search->highlightPostTag),
                1,
            );
        }

        $searchQueries = $this->extractSearchQueries($search->filters);
        $nonSearchFilters = $this->extractNonSearchFilters($search->filters);

        $pipeline = [];

        if ([] !== $searchQueries) {
            $pipeline[] = [
                '$search' => $this->createSearchStage($search, $searchQueries),
            ];

            if ([] !== $search->highlightFields) {
                $pipeline[] = [
                    '$addFields' => [
                        '_searchHighlights' => ['$meta' => 'searchHighlights'],
                    ],
                ];
            }

            $match = $this->recursiveResolveFilterConditions($search->index, $nonSearchFilters, true);
            if ([] !== $match) {
                $pipeline[] = ['$match' => $match];
            }
        } else {
            $match = $this->recursiveResolveFilterConditions($search->index, $nonSearchFilters, true);
            $pipeline[] = ['$match' => [] !== $match ? $match : new \stdClass()];
        }

        $documentsPipeline = $this->createDocumentsFacetPipeline($search);
        $totalPipeline = $this->createTotalFacetPipeline($search);

        $facetPipelines = [
            'documents' => $documentsPipeline,
            'total' => $totalPipeline,
        ];

        foreach ($search->facets as $facet) {
            $facetPipelines[$this->createFacetIdentifier($facet)] = $this->createFacetPipeline($search->index, $facet);
        }

        $pipeline[] = [
            '$facet' => $facetPipelines,
        ];

        $aggregationResult = $this->executeAggregation($search->index, $pipeline, [] !== $searchQueries);

        $searchResults = $this->extractDocuments($aggregationResult);
        $total = $this->extractTotal($aggregationResult);

        return new Result(
            $this->hitsToDocuments(
                $search->index,
                $searchResults,
                $search->highlightFields,
                $search->highlightPreTag,
                $search->highlightPostTag,
            ),
            $total,
            $this->formatFacets($search->facets, $aggregationResult),
        );
    }

    /**
     * @param array<int, array<string, mixed>> $pipeline
     *
     * @return array<string, mixed>
     */
    private function executeAggregation(Index $index, array $pipeline, bool $retrySearchRefresh): array
    {
        $collection = $this->client->getDatabase()->getCollection($index->name);

        for ($attempt = 0;; ++$attempt) {
            /** @var array<int, array<string, mixed>> $aggregationResults */
            $aggregationResults = $collection->aggregate($pipeline, [
                'typeMap' => [
                    'root' => 'array',
                    'document' => 'array',
                ],
            ])->toArray();

            $aggregationResult = $aggregationResults[0] ?? [];

            if (!$retrySearchRefresh) {
                return $aggregationResult;
            }

            $searchResults = $this->extractDocuments($aggregationResult);
            $total = $this->extractTotal($aggregationResult);

            if ([] !== $searchResults || 0 !== $total) {
                return $aggregationResult;
            }

            if ($attempt >= 12) {
                return $aggregationResult;
            }

            if (0 === $collection->countDocuments()) {
                return $aggregationResult;
            }

            \usleep(100_000);
        }
    }

    /**
     * @param array<string, mixed> $aggregationResult
     *
     * @return array<int, array<string, mixed>>
     */
    private function extractDocuments(array $aggregationResult): array
    {
        $documents = $aggregationResult['documents'] ?? null;
        if (!\is_array($documents)) {
            return [];
        }

        $result = [];
        foreach ($documents as $document) {
            if (\is_array($document)) {
                /** @var array<string, mixed> $document */
                $result[] = $document;
            }
        }

        return $result;
    }

    /**
     * @param array<string, mixed> $aggregationResult
     */
    private function extractTotal(array $aggregationResult): int
    {
        $total = $aggregationResult['total'] ?? null;
        if (!\is_array($total)) {
            return 0;
        }

        $firstTotal = $total[0] ?? null;
        if (!\is_array($firstTotal)) {
            return 0;
        }

        $value = $firstTotal['value'] ?? null;
        if (!\is_int($value) && !\is_float($value) && !\is_string($value)) {
            return 0;
        }

        return (int) $value;
    }

    /**
     * @param object[] $filters
     *
     * @return array<string>
     */
    private function extractSearchQueries(array $filters): array
    {
        $queries = [];
        foreach ($filters as $filter) {
            if ($filter instanceof Condition\SearchCondition) {
                $queries[] = $filter->query;
            }
        }

        return $queries;
    }

    /**
     * @param object[] $filters
     *
     * @return object[]
     */
    private function extractNonSearchFilters(array $filters): array
    {
        return \array_values(\array_filter(
            $filters,
            static fn (object $filter): bool => !$filter instanceof Condition\SearchCondition,
        ));
    }

    /**
     * @param array<string> $queries
     *
     * @return array<string, mixed>
     */
    private function createSearchStage(Search $search, array $queries): array
    {
        $must = [];
        foreach ($queries as $query) {
            $must[] = [
                'text' => [
                    'query' => $query,
                    'path' => $search->index->searchableFields,
                ],
            ];
        }

        $searchStage = [
            'index' => $search->index->name . '-search-index',
            'compound' => [
                'must' => $must,
            ],
        ];

        if ([] !== $search->highlightFields) {
            $searchStage['highlight'] = [
                'path' => $search->highlightFields,
            ];
        }

        return $searchStage;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function createDocumentsFacetPipeline(Search $search): array
    {
        $pipeline = [];

        if ([] !== $search->sortBys) {
            $sort = [];
            foreach ($search->sortBys as $field => $direction) {
                $sort[$this->getFilterField($search->index, $field)] = 'asc' === $direction ? 1 : -1;
            }
            $pipeline[] = ['$sort' => $sort];
        }

        if (null !== $search->distinct) {
            $distinctField = '$' . $this->getFilterField($search->index, $search->distinct);
            $pipeline[] = [
                '$group' => [
                    '_id' => $distinctField,
                    'document' => [
                        '$first' => '$$ROOT',
                    ],
                ],
            ];
            $pipeline[] = [
                '$replaceRoot' => [
                    'newRoot' => '$document',
                ],
            ];

            if ([] !== $search->sortBys) {
                $sort = [];
                foreach ($search->sortBys as $field => $direction) {
                    $sort[$this->getFilterField($search->index, $field)] = 'asc' === $direction ? 1 : -1;
                }
                $pipeline[] = ['$sort' => $sort];
            }
        }

        if (0 !== $search->offset) {
            $pipeline[] = ['$skip' => $search->offset];
        }

        if (null !== $search->limit) {
            $pipeline[] = ['$limit' => $search->limit];
        }

        return $pipeline;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function createTotalFacetPipeline(Search $search): array
    {
        $pipeline = [];

        if (null !== $search->distinct) {
            $pipeline[] = [
                '$group' => [
                    '_id' => '$' . $this->getFilterField($search->index, $search->distinct),
                ],
            ];
        }

        $pipeline[] = [
            '$count' => 'value',
        ];

        return $pipeline;
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function createFacetPipeline(Index $index, AbstractFacet $facet): array
    {
        if ($facet instanceof CountFacet) {
            $pipeline = [
                [
                    '$match' => [
                        $facet->field => [
                            '$exists' => true,
                            '$ne' => null,
                        ],
                    ],
                ],
            ];

            $field = $index->findFieldByPath($facet->field);
            if ($field?->multiple) {
                $pipeline[] = [
                    '$unwind' => '$' . $facet->field,
                ];
            }

            $pipeline[] = [
                '$group' => [
                    '_id' => '$' . $facet->field,
                    'count' => [
                        '$sum' => 1,
                    ],
                ],
            ];

            return $pipeline;
        }

        \assert($facet instanceof MinMaxFacet);

        return [
            [
                '$match' => [
                    $facet->field => [
                        '$exists' => true,
                        '$ne' => null,
                    ],
                ],
            ],
            [
                '$group' => [
                    '_id' => null,
                    'min' => [
                        '$min' => '$' . $facet->field,
                    ],
                    'max' => [
                        '$max' => '$' . $facet->field,
                    ],
                ],
            ],
        ];
    }

    private function createFacetIdentifier(AbstractFacet $facet): string
    {
        return match (true) {
            $facet instanceof CountFacet => 'facet_count_' . $facet->field,
            $facet instanceof MinMaxFacet => 'facet_min_max_' . $facet->field,
            default => throw new \LogicException($facet::class . ' facet not implemented.'),
        };
    }

    /**
     * @param array<AbstractFacet> $facets
     * @param array<string, mixed> $aggregationResult
     *
     * @return array<string, mixed>
     */
    private function formatFacets(array $facets, array $aggregationResult): array
    {
        $formatted = [];

        foreach ($facets as $facet) {
            $facetIdentifier = $this->createFacetIdentifier($facet);
            $facetAggregationResult = $aggregationResult[$facetIdentifier] ?? [];

            if (!\is_array($facetAggregationResult)) {
                continue;
            }

            if ($facet instanceof CountFacet) {
                $formatted[$facet->field]['count'] = [];

                foreach ($facetAggregationResult as $countResult) {
                    if (!\is_array($countResult) || !isset($countResult['count'])) {
                        continue;
                    }

                    $idValue = $countResult['_id'] ?? null;
                    $key = match (true) {
                        \is_bool($idValue) => $idValue ? 'true' : 'false',
                        null === $idValue => 'null',
                        \is_int($idValue), \is_float($idValue), \is_string($idValue) => (string) $idValue,
                        $idValue instanceof \Stringable => (string) $idValue,
                        default => \json_encode($idValue, \JSON_THROW_ON_ERROR),
                    };

                    $count = $countResult['count'];
                    if (!\is_int($count) && !\is_float($count) && !\is_string($count)) {
                        continue;
                    }

                    $formatted[$facet->field]['count'][$key] = (int) $count;
                }

                continue;
            }

            \assert($facet instanceof MinMaxFacet);

            $facetResult = $facetAggregationResult[0] ?? [];
            if (!\is_array($facetResult)) {
                continue;
            }

            if (isset($facetResult['min'])) {
                $formatted[$facet->field]['min'] = $facetResult['min'];
            }

            if (isset($facetResult['max'])) {
                $formatted[$facet->field]['max'] = $facetResult['max'];
            }
        }

        return $formatted;
    }

    /**
     * @param iterable<array<string, mixed>> $hits
     * @param array<string> $highlightFields
     *
     * @return \Generator<int, array<string, mixed>>
     */
    private function hitsToDocuments(
        Index $index,
        iterable $hits,
        array $highlightFields,
        string $highlightPreTag,
        string $highlightPostTag,
    ): \Generator {
        foreach ($hits as $hit) {
            $this->normalizeGeoPointField($index, $hit);

            $identifier = $this->normalizeIdentifier($hit['_id'] ?? null);
            unset($hit['_id']);

            if (null === $identifier) {
                continue;
            }

            $hit[$index->getIdentifierField()->name] = $identifier;

            $document = $this->marshaller->unmarshall($index->fields, $hit);

            if ([] === $highlightFields) {
                yield $document;

                continue;
            }

            $document['_formatted'] ??= [];

            \assert(
                \is_array($document['_formatted']),
                'Document with key "_formatted" expected to be array.',
            );

            foreach ($highlightFields as $highlightField) {
                $document['_formatted'][$highlightField] = $this->resolveHighlightValue(
                    $hit['_searchHighlights'] ?? null,
                    $highlightField,
                    $highlightPreTag,
                    $highlightPostTag,
                );
            }

            yield $document;
        }
    }

    private function normalizeIdentifier(mixed $identifier): string|int|null
    {
        return match (true) {
            \is_string($identifier), \is_int($identifier) => $identifier,
            $identifier instanceof \Stringable => (string) $identifier,
            default => null,
        };
    }

    /**
     * @param array<string, mixed> $hit
     */
    private function normalizeGeoPointField(Index $index, array &$hit): void
    {
        $geoPointField = $index->getGeoPointField();

        if (!$geoPointField instanceof GeoPointField || !isset($hit[$geoPointField->name]) || !\is_array($hit[$geoPointField->name])) {
            return;
        }

        $geoValue = $hit[$geoPointField->name];
        $coordinates = $geoValue['coordinates'] ?? null;

        if (!\is_array($coordinates) || !isset($coordinates[0], $coordinates[1])) {
            return;
        }

        if (!\is_numeric($coordinates[0]) || !\is_numeric($coordinates[1])) {
            return;
        }

        $hit[$geoPointField->name] = [
            'lat' => (float) $coordinates[1],
            'lon' => (float) $coordinates[0],
        ];
    }

    private function resolveHighlightValue(
        mixed $highlights,
        string $field,
        string $highlightPreTag,
        string $highlightPostTag,
    ): string|null {
        if (!\is_array($highlights)) {
            return null;
        }

        foreach ($highlights as $highlight) {
            if (!\is_array($highlight)) {
                continue;
            }

            $path = $highlight['path'] ?? null;
            $highlightPath = match (true) {
                \is_string($path) => $path,
                \is_array($path) => \implode('.', \array_map(static fn (mixed $value): string => match (true) {
                    \is_string($value), \is_int($value), \is_float($value) => (string) $value,
                    $value instanceof \Stringable => (string) $value,
                    default => '',
                }, $path)),
                default => null,
            };

            if ($field !== $highlightPath) {
                continue;
            }

            $texts = $highlight['texts'] ?? null;
            if (!\is_array($texts)) {
                return null;
            }

            $result = '';
            foreach ($texts as $text) {
                if (!\is_array($text)) {
                    continue;
                }

                $valueRaw = $text['value'] ?? '';
                $value = match (true) {
                    \is_string($valueRaw), \is_int($valueRaw), \is_float($valueRaw) => (string) $valueRaw,
                    $valueRaw instanceof \Stringable => (string) $valueRaw,
                    default => '',
                };
                $type = $text['type'] ?? 'text';

                if ('hit' === $type) {
                    $result .= $highlightPreTag . $value . $highlightPostTag;

                    continue;
                }

                $result .= $value;
            }

            return '' !== $result ? $result : null;
        }

        return null;
    }

    private function getFilterField(Index $index, string $name): string
    {
        return $name === $index->getIdentifierField()->name ? '_id' : $name;
    }

    /**
     * @param object[] $filters
     *
     * @return array<string, mixed>
     */
    private function recursiveResolveFilterConditions(Index $index, array $filters, bool $conjunctive): array
    {
        $filterQueries = [];

        foreach ($filters as $filter) {
            match (true) {
                $filter instanceof Condition\IdentifierCondition => $filterQueries[] = [
                    '_id' => $filter->identifier,
                ],
                $filter instanceof Condition\SearchCondition => null,
                $filter instanceof Condition\EqualCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => $filter->value,
                ],
                $filter instanceof Condition\NotEqualCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$ne' => $filter->value,
                    ],
                ],
                $filter instanceof Condition\GreaterThanCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$gt' => $filter->value,
                    ],
                ],
                $filter instanceof Condition\GreaterThanEqualCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$gte' => $filter->value,
                    ],
                ],
                $filter instanceof Condition\LessThanCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$lt' => $filter->value,
                    ],
                ],
                $filter instanceof Condition\LessThanEqualCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$lte' => $filter->value,
                    ],
                ],
                $filter instanceof Condition\InCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$in' => $filter->values,
                    ],
                ],
                $filter instanceof Condition\NotInCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$nin' => $filter->values,
                    ],
                ],
                $filter instanceof Condition\GeoDistanceCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$geoWithin' => [
                            '$centerSphere' => [
                                [
                                    $filter->longitude,
                                    $filter->latitude,
                                ],
                                $filter->distance / 6_371_000,
                            ],
                        ],
                    ],
                ],
                $filter instanceof Condition\GeoBoundingBoxCondition => $filterQueries[] = [
                    $this->getFilterField($index, $filter->field) => [
                        '$geoWithin' => [
                            '$geometry' => [
                                'type' => 'Polygon',
                                'coordinates' => [[
                                    [$filter->westLongitude, $filter->southLatitude],
                                    [$filter->eastLongitude, $filter->southLatitude],
                                    [$filter->eastLongitude, $filter->northLatitude],
                                    [$filter->westLongitude, $filter->northLatitude],
                                    [$filter->westLongitude, $filter->southLatitude],
                                ]],
                            ],
                        ],
                    ],
                ],
                $filter instanceof Condition\AndCondition => $filterQueries[] = $this->recursiveResolveFilterConditions($index, $filter->conditions, true),
                $filter instanceof Condition\OrCondition => $filterQueries[] = $this->recursiveResolveFilterConditions($index, $filter->conditions, false),
                default => throw new \LogicException($filter::class . ' filter not implemented.'),
            };
        }

        $filterQueries = \array_values(\array_filter(
            $filterQueries,
            static fn (array $query): bool => [] !== $query,
        ));

        if (\count($filterQueries) <= 1) {
            return $filterQueries[0] ?? [];
        }

        return [
            $conjunctive ? '$and' : '$or' => $filterQueries,
        ];
    }
}
