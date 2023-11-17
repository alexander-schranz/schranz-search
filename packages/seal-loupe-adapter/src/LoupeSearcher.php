<?php

declare(strict_types=1);

/*
 * This file is part of the Schranz Search package.
 *
 * (c) Alexander Schranz <alexander@sulu.io>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Schranz\Search\SEAL\Adapter\Loupe;

use Loupe\Loupe\SearchParameters;
use Schranz\Search\SEAL\Adapter\SearcherInterface;
use Schranz\Search\SEAL\Marshaller\FlattenMarshaller;
use Schranz\Search\SEAL\Schema\Index;
use Schranz\Search\SEAL\Search\Condition;
use Schranz\Search\SEAL\Search\Result;
use Schranz\Search\SEAL\Search\Search;

final class LoupeSearcher implements SearcherInterface
{
    private readonly FlattenMarshaller $marshaller;

    public function __construct(
        private readonly LoupeHelper $loupeHelper,
    ) {
        $this->marshaller = new FlattenMarshaller(
            dateAsInteger: true,
            separator: LoupeHelper::SEPERATOR,
            sourceField: LoupeHelper::SOURCE_FIELD,
        );
    }

    public function search(Search $search): Result
    {
        // optimized single document query
        if (
            1 === \count($search->indexes)
            && 1 === \count($search->filters)
            && $search->filters[0] instanceof Condition\IdentifierCondition
            && 0 === $search->offset
            && 1 === $search->limit
        ) {
            $loupe = $this->loupeHelper->getLoupe($search->indexes[\array_key_first($search->indexes)]);
            $data = $loupe->getDocument($search->filters[0]->identifier);

            if (!$data) {
                return new Result(
                    $this->hitsToDocuments($search->indexes, []),
                    0,
                );
            }

            return new Result(
                $this->hitsToDocuments($search->indexes, [$data]),
                1,
            );
        }

        if (1 !== \count($search->indexes)) {
            throw new \RuntimeException('Meilisearch does not yet support search multiple indexes: https://github.com/schranz-search/schranz-search/issues/28');
        }

        $index = $search->indexes[\array_key_first($search->indexes)];

        $loupe = $this->loupeHelper->getLoupe($index);

        $searchParameters = SearchParameters::create();

        $query = null;
        $filters = [];
        foreach ($search->filters as $filter) {
            match (true) {
                $filter instanceof Condition\IdentifierCondition => $filters[] = $index->getIdentifierField()->name . ' = ' . $this->escapeFilterValue($filter->identifier),
                $filter instanceof Condition\SearchCondition => $query = $filter->query,
                $filter instanceof Condition\EqualCondition => $filters[] = $filter->field . ' = ' . $this->escapeFilterValue($filter->value),
                $filter instanceof Condition\NotEqualCondition => $filters[] = $filter->field . ' != ' . $this->escapeFilterValue($filter->value),
                $filter instanceof Condition\GreaterThanCondition => $filters[] = $filter->field . ' > ' . $this->escapeFilterValue($filter->value),
                $filter instanceof Condition\GreaterThanEqualCondition => $filters[] = $filter->field . ' >= ' . $this->escapeFilterValue($filter->value),
                $filter instanceof Condition\LessThanCondition => $filters[] = $filter->field . ' < ' . $this->escapeFilterValue($filter->value),
                $filter instanceof Condition\LessThanEqualCondition => $filters[] = $filter->field . ' <= ' . $this->escapeFilterValue($filter->value),
                default => throw new \LogicException($filter::class . ' filter not implemented.'),
            };
        }

        if ($query) {
            $searchParameters = $searchParameters->withQuery($query);
        }

        if ([] !== $filters) {
            $searchParameters = $searchParameters->withFilter(\implode(' AND ', $filters));
        }

        if ($search->limit) {
            $searchParameters = $searchParameters->withHitsPerPage($search->limit);
        }

        if ($search->offset && $search->limit && 0 === ($search->offset % $search->limit)) {
            $searchParameters = $searchParameters->withPage((int) (($search->offset / $search->limit) + 1));
        } elseif (null !== $search->limit && 0 !== $search->offset) {
            throw new \RuntimeException('None paginated limit and offset not supported. See https://github.com/loupe-php/loupe/issues/13');
        }

        $sorts = [];
        foreach ($search->sortBys as $field => $direction) {
            $sorts[] = $field . ':' . $direction;
        }

        if ([] !== $sorts) {
            $searchParameters = $searchParameters->withSort($sorts);
        }

        $result = $loupe->search($searchParameters);

        return new Result(
            $this->hitsToDocuments($search->indexes, $result->getHits()),
            $result->getTotalHits(),
        );
    }

    private function escapeFilterValue(string|int|float|bool $value): string
    {
        // TODO replace with SearchParameters::escapeFilterValue once updated Loupe to 0.5
        //      see also https://github.com/loupe-php/loupe/pull/54
        return match (true) {
            \is_bool($value) => $value ? '1' : '0',
            \is_int($value), \is_float($value) => (string) $value,
            default => "'" . \str_replace("'", "''", $value) . "'"
        };
    }

    /**
     * @param Index[] $indexes
     * @param iterable<array<string, mixed>> $hits
     *
     * @return \Generator<int, array<string, mixed>>
     */
    private function hitsToDocuments(array $indexes, iterable $hits): \Generator
    {
        $index = $indexes[\array_key_first($indexes)];

        foreach ($hits as $hit) {
            yield $this->marshaller->unmarshall($index->fields, $hit);
        }
    }
}
