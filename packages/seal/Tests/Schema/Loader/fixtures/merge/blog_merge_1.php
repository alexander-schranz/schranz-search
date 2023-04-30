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

use Schranz\Search\SEAL\Schema\Field;
use Schranz\Search\SEAL\Schema\Index;

return new Index('blog', [
    'id' => new Field\IdentifierField('id'),
    'title' => new Field\TextField('title'),
    'description' => new Field\TextField('description', options: ['option1' => true]),
    'blocks' => new Field\TypedField('blocks', 'type', [
        'text' => [
            'title' => new Field\TextField('title'),
            'description' => new Field\TextField('description'),
            'media' => new Field\IntegerField('media', multiple: true),
        ],
        'embed' => [
            'title' => new Field\TextField('title'),
            'media' => new Field\TextField('media'),
        ],
    ], multiple: true),
]);
