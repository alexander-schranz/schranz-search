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

namespace Schranz\Search\SEAL\Search\Condition;

class GeoDistanceCondition
{
    /**
     * @param int $distance search radius in meters
     */
    public function __construct(
        public readonly string $field,
        public readonly float $latitude,
        public readonly float $longitude,
        public readonly int $distance,
    ) {
    }
}