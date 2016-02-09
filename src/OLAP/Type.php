<?php

namespace OLAP;


class Type {

    private $type = '';
    private $create = '';

    public function __construct($options) {

        $this->type = is_string($options) ? $options : (empty($options['name']) ? 'text' : $options['name']);
        $this->create = empty($options['create']) ? '' : $options['create'];
    }

    public function getType() {

        return $this->type;
    }

    public function getCreation() {

        return $this->create;
    }
}