<?php

namespace Schranz\Search\SEAL\Adapter\Meilisearch;

use Meilisearch\Client;
use Schranz\Search\SEAL\Adapter\IndexerInterface;
use Schranz\Search\SEAL\Marshaller\Marshaller;
use Schranz\Search\SEAL\Schema\Index;
use Schranz\Search\SEAL\Task\AsyncTask;
use Schranz\Search\SEAL\Task\TaskInterface;

final class MeilisearchIndexer implements IndexerInterface
{
    private Marshaller $marshaller;

    public function __construct(
        private readonly Client $client,
    ) {
        $this->marshaller = new Marshaller();
    }

    public function save(Index $index, array $document, array $options = []): ?TaskInterface
    {
        $identifierField = $index->getIdentifierField();

        /** @var string|null $identifier */
        $identifier = ((string) $document[$identifierField->name]) ?? null;

        $indexResponse = $this->client->index($index->name)->addDocuments([
            $this->marshaller->marshall($index->fields, $document),
        ], $identifierField->name);

        if ($indexResponse['status'] !== 'enqueued') {
            throw new \RuntimeException('Unexpected error while save document with identifier "' . $identifier . '" into Index "' . $index->name . '".');
        }

        if (true !== ($options['return_slow_promise_result'] ?? false)) {
            return null;
        }

        return new AsyncTask(function() use ($indexResponse) {
            $this->client->waitForTask($indexResponse['taskUid']);
        });
    }

    public function delete(Index $index, string $identifier, array $options = []): ?TaskInterface
    {
        $deleteResponse = $this->client->index($index->name)->deleteDocument($identifier);

        if ($deleteResponse['status'] !== 'enqueued') {
            throw new \RuntimeException('Unexpected error while delete document with identifier "' . $identifier . '" from Index "' . $index->name . '".');
        }

        if (true !== ($options['return_slow_promise_result'] ?? false)) {
            return null;
        }

        return new AsyncTask(function() use ($deleteResponse) {
            $this->client->waitForTask($deleteResponse['taskUid']);
        });
    }
}