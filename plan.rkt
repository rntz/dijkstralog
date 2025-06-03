#lang racket

(require racket/set)

(define-syntax-rule (todo) (error "todo"))
(define-syntax-rule (assert! test)
  (unless test
    (error "ASSERTION FAILURE:" 'test)))

(define atom-pred car)
(define atom-args cdr)

(define var? symbol?)

;; A notion of subsequence that allows terms to be duplicated in xs. We assume
;; terms are not duplicated in ys but do not check this. We can use a term from
;; ys multiple times. eg:
;;
;; (subsequence '(a c) '(a b c))   ==> (0 2)
;; (subsequence '(b b) '(a b c))   ==> (1 1)
;; (subsequence '(b a) '(a b c))   ==> #f
;; (subsequence '(a b a) '(a b c)) ==> #f
;; (subsequence '(a b b) '(a b c)) ==> (0 1 1)
(define (subsequence xs ys #:by [equal equal?])
  (let loop ([acc '()] [i 0] [xs xs] [ys ys])
    (match* (xs ys)
      [('() _) (reverse acc)]
      [(_ '()) #f]
      [(xs ys)
       (if (equal (car xs) (car ys))
           (loop (cons i acc) i       (cdr xs) ys)
           (loop acc          (+ 1 i) xs       (cdr ys)))])))

;; (compile var-order query) --> (values query-plan decls)
;; decls: a list of indices that we need for this query
(define (compile var-order query)
  (assert! (= (length var-order) (set-count (list->set var-order))))
  (assert! (for*/and ([atom query] [x (atom-args atom)] #:when (var? x))
             (member x var-order)))

  ;; NOTE/TODO: we could get rid of constants and duplicated variables up front
  ;; (as operations on trie iterators), instead of pushing that work through.
  ;; That would avoid the need to handle it separately in each of
  ;; sorting-and-projection and conjunctive-querying.
  (define decls '())
  (define query-sorted
    (for/list ([atom query])
      (define atom-vars (filter var? (atom-args atom)))
      (if (subsequence atom-vars var-order) atom
          ;; Otherwise, need to re-sort!
          (let* ([new-pred (gensym (atom-pred atom))]
                 [new-vars (filter (lambda (x) (member x atom-vars)) var-order)]
                 [new-atom `(,new-pred ,@new-vars)]
                 ;; TODO: re-use indices via hash-consing re-sortings
                 [sort-decl `(sort ,new-atom ,atom)])
            (set! decls (cons sort-decl decls))
            new-atom))))

  ;; mutable local vars for planning query
  (define vars-seen (mutable-set))
  (define reverse-query-plan '())
  (define iter-states ;; tracks each trie iterator's progress into its relation
    (for/list ([atom query-sorted])
      (mcons (atom-pred atom) (atom-args atom))))

  (define (plan-push! x) (set! reverse-query-plan (cons x reverse-query-plan)))
  (define (lookups!)
    (for ([iter-state iter-states])
      (define-values (lookups leftover)
        (splitf-at (mcdr iter-state)
                   (λ (x) (or (not (var? x)) (set-member? vars-seen x)))))
      (set-mcdr! iter-state leftover)
      (for ([x lookups])
        (plan-push! `(lookup ,(mcar iter-state) ,x)))))

  ;; Plan the query.
  (lookups!)
  (for ([var var-order])
    (set-add! vars-seen var)
    ;; Find the relations which touch this variable and inner join them.
    (define join-iters
      (for/list ([iter-state iter-states]
                 #:do [(match-define (mcons iter iter-args) iter-state)]
                 #:when (not (null? iter-args))
                 #:when (equal? var (car iter-args)))
        (set-mcdr! iter-state (cdr iter-args))
        iter))
    (plan-push! `(,var <- join ,@join-iters))
    (lookups!))

  (values (reverse reverse-query-plan) decls))

;; Compiles a single atom to a query plan, deriving the variable order from the
;; atom. Doesn't need any additional decls. Used for planning sorts.
(define (compile-atom atom)
  (define vars-seen (mutable-set))
  (define var-order
    (for/list ([x (atom-args atom)]
               #:when (var? x)
               #:when (not (set-member? vars-seen x)))
      (set-add! vars-seen x)
      x))
  (define-values (plan decls) (compile var-order `(,atom)))
  (assert! (null? decls))
  plan)

(module+ tests
  ;; triangle query with a lookup in T.
  (compile '(a b c) '((R a b) (S b c) (T a 17 c)))
  ;; make sure it generates a sort-and-project step for T.
  (compile '(a b c) '((R a b) (S b c) (T c 17 a)))
  ;; make sure it looks up the second b in S.
  (compile '(a b c) '((R a b) (S b b) (T a 17 c)))

  ;; make sure we can compile a sort body.
  (compile-atom '(T c 17 a))
  )
