<?php

namespace CmsIg\Seal\Search\Facet;

/**
 * @readonly
 */
abstract class AbstractFacet
{
    /**
     * @param array<string, mixed> $options
     */
    public function __construct(
        public readonly string $field,
        public readonly array $options = [],
    ) {
    }
}
